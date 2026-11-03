#!/usr/bin/env python3
"""SE363: mở rộng dữ liệu có provenance, không giả danh RSS thành quảng cáo.

Modes:
  ner: download pinned ViEcomNER research dataset under data/external (not ads)
  rss: collect publicly published RSS titles/summaries from vetted source list (not full articles)
  report: count distinct records by source class
  self-test: local test only; creates no dataset files

No captcha/login circumvention, no page spidering. 429/403 are reported and not retried.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
from html import unescape
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
RAW = ROOT / 'data' / 'raw'
EXTERNAL = ROOT / 'data' / 'external' / 'ViEcomNER'
REPORTS = ROOT / 'data' / 'reports'
SOURCE_CONFIG = ROOT / 'sources' / 'rss_sources_se363.json'
HF_REPO = 'tnhyen/vietnamese-ecommerce-ner'
HF_REVISION = 'c8ba175c54b08015a40a9b4658504a02a08b7d8b'
HF_BASE = f'https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}'
USER_AGENT = 'SE363-AcademicDataCollector/1.0 (RSS summary and permitted dataset only; noncommercial research)'


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def sha(s):
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def request_bytes(url, max_bytes=10 * 1024 * 1024):
    req = Request(url, headers={'User-Agent': USER_AGENT, 'Accept': 'application/xml,text/xml,application/atom+xml,application/rss+xml,application/json,text/plain,*/*'})
    with urlopen(req, timeout=30) as res:
        body = res.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError(f'Payload > {max_bytes} bytes: {url}')
    return body


class ToText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, d):
        self.parts.append(d)


def plain(s):
    obj = ToText()
    obj.feed(unescape(s or ''))
    return ' '.join(' '.join(obj.parts).split())


def lname(tag):
    return tag.rsplit('}', 1)[-1]


def field(item, names):
    for name in names:
        for child in item:
            if lname(child.tag) == name:
                if name == 'link' and child.get('href'):
                    return child.get('href')
                return ''.join(child.itertext()).strip()
    return ''


def parse_rss(raw):
    raw = raw.lstrip()
    if raw.startswith(b'\xef\xbb\xbf'):
        raw = raw[3:].lstrip()
    root = ET.fromstring(raw)
    records = []
    for node in root.iter():
        if lname(node.tag) not in ('item', 'entry'):
            continue
        title = plain(field(node, ['title']))
        link = field(node, ['link'])
        summary = plain(field(node, ['description', 'summary', 'encoded', 'content']))
        pub = field(node, ['pubDate', 'published', 'updated'])
        if link.startswith(('https://', 'http://')) and (title or summary):
            records.append({'title': title, 'url': link, 'summary': summary, 'published_at': pub})
    return records


def canonical(url):
    u = urlsplit(url)
    keep = [(k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
            if k.lower() not in {'fbclid','gclid','ref','source'} and not k.lower().startswith('utm_')]
    return urlunsplit((u.scheme.lower(), u.netloc.lower(), u.path.rstrip('/') or '/', urlencode(keep), ''))


def load_jsonl(path):
    if not path.exists():
        return
    with path.open('r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                print(f'[WARN] invalid JSONL {path.name}:{i}', file=sys.stderr)


def export_csv(src):
    rows = list(load_jsonl(src))
    if not rows:
        return
    cols = list(dict.fromkeys(key for row in rows for key in row))
    with src.with_suffix('.csv').open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def collect_rss(limit=50, sleep_seconds=2.5):
    if not SOURCE_CONFIG.exists():
        raise FileNotFoundError(f'Missing RSS config: {SOURCE_CONFIG}')
    feeds = json.loads(SOURCE_CONFIG.read_text(encoding='utf-8'))
    if not isinstance(feeds, list):
        raise ValueError('RSS config must be JSON array')
    limit = max(1, min(limit, 50))
    RAW.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    seen = {}
    for key, dest in [('promotion_candidate', RAW / 'posts_auto.jsonl'),
                      ('evidence_candidate', RAW / 'evidence_discovery.jsonl')]:
        seen[key] = set()
        for obj in load_jsonl(dest):
            url = obj.get('source_url') or obj.get('url') or ''
            if url:
                seen[key].add(canonical(url))
    report = {'run_at': now(), 'mode': 'rss', 'limits': 'RSS summary/title only; noncommercial use; NOT original ad verification; NOT independently confirmed evidence', 'feeds': [], 'errors': []}
    touched = set()
    for idx, feed in enumerate(feeds):
        if idx:
            time.sleep(max(0.0, sleep_seconds))
        role = feed.get('role', '')
        url = feed.get('url', '')
        if not feed.get('authorized_for_collection') or role not in seen or not url.startswith('https://'):
            report['feeds'].append({'url': url, 'skipped': True, 'reason': 'rights/role/url not approved'})
            continue
        try:
            data = request_bytes(url, max_bytes=2 * 1024 * 1024)
            items = parse_rss(data)
            out = RAW / ('posts_auto.jsonl' if role == 'promotion_candidate' else 'evidence_discovery.jsonl')
            newrows = []
            for item in items[:limit]:
                norm_url = canonical(item['url'])
                if norm_url in seen[role]:
                    continue
                observed = now()
                base = {
                    '_key': 'se363feed:' + sha(role + '|' + norm_url),
                    'source_role': 'promotional_content_candidate' if role == 'promotion_candidate' else 'evidence_candidate',
                    'source_type': 'publisher_store_rss_candidate' if role == 'promotion_candidate' else 'publisher_rss_evidence_index',
                    'publisher': feed['publisher'], 'title': item['title'], 'source_url': item['url'], 'url': item['url'],
                    'published_at': item['published_at'], 'observed_at': observed, 'rss_feed_url': url,
                    'terms_url': feed['terms_url'], 'record_status': 'needs_review',
                    'usage_note': 'Metadata and RSS-supplied summary only. Cite source. No full article republishing.',
                }
                if role == 'promotion_candidate':
                    base.update({'post_id': 'RSS-' + sha(norm_url)[:12], 'platform': 'publisher_website_rss',
                                 'advertiser': '', 'raw_text': item['summary'],
                                 'content_hash_sha256': sha(item['summary']),
                                 'note': 'Promotion candidate from publisher Store; NOT verified original/paid brand advertisement.'})
                else:
                    base.update({'summary': item['summary'], 'note': 'Reference discovery only; NOT original ad and NOT verified evidence.'})
                newrows.append(base)
                seen[role].add(norm_url)
            with out.open('a', encoding='utf-8') as f:
                for row in newrows:
                    f.write(json.dumps(row, ensure_ascii=False) + '\n')
            touched.add(out)
            print(f'[RSS] {feed["publisher"]}: found={len(items)} new={len(newrows)} role={role}')
            report['feeds'].append({'url': url, 'role': role, 'found': len(items), 'new': len(newrows)})
        except (HTTPError, URLError, OSError, ET.ParseError, ValueError, KeyError) as exc:
            reason = f'HTTP {exc.code}' if isinstance(exc, HTTPError) else str(exc)
            report['errors'].append({'url': url, 'error': reason[:300]})
            print(f'[RSS ERROR] {url}: {reason[:180]}')
    for dest in touched:
        export_csv(dest)
    output = REPORTS / 'rss_expansion_report.json'
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'RSS report: {output.relative_to(ROOT)}')
    return report


def download_ner():
    EXTERNAL.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    report = {'run_at': now(), 'dataset': HF_REPO, 'revision': HF_REVISION,
              'license': 'CC-BY-NC-SA-4.0 (repository card; use for noncommercial research and give attribution)',
              'type': 'annotated product TITLE NER; NOT advertisements or product regulatory metadata',
              'files': [], 'errors': []}
    for name in ('train', 'validation', 'test'):
        filename = f'{name}.jsonl'
        dst = EXTERNAL / filename
        if dst.exists() and dst.stat().st_size:
            print(f'[NER] {filename}: already exists, checking; NOT re-downloading')
        else:
            url = f'{HF_BASE}/{filename}'
            try:
                data = request_bytes(url, max_bytes=10 * 1024 * 1024)
                # Do not save a rate-limit page or HTML instead of a dataset.
                first = next((ln for ln in data.splitlines() if ln.strip()), b'')
                example = json.loads(first)
                if not isinstance(example, dict) or 'text' not in example:
                    raise ValueError(f'{filename} missing expected text field')
                tmp = dst.with_suffix('.tmp')
                tmp.write_bytes(data)
                tmp.replace(dst)
                print(f'[NER] Downloaded: {filename} ({len(data)} bytes)')
            except (HTTPError, URLError, OSError, ValueError, json.JSONDecodeError) as exc:
                err = f'HTTP {exc.code}' if isinstance(exc, HTTPError) else str(exc)
                print(f'[NER ERROR] {filename}: {err}')
                report['errors'].append({'file': filename, 'error': err})
                continue
        count = 0
        invalid = 0
        missing_text = 0
        with dst.open(encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                    count += 1
                    if not rec.get('text'):
                        missing_text += 1
                except (ValueError, AttributeError):
                    invalid += 1
        report['files'].append({'file': str(dst.relative_to(ROOT)), 'rows': count, 'invalid_lines': invalid,
                                'missing_text': missing_text, 'sha256': hashlib.sha256(dst.read_bytes()).hexdigest()})
        print(f'[NER] {filename}: rows={count}, invalid={invalid}, missing text={missing_text}')
    (EXTERNAL / 'PROVENANCE.json').write_text(json.dumps({
        'repository': 'https://huggingface.co/datasets/' + HF_REPO, 'revision': HF_REVISION,
        'license_url': 'https://creativecommons.org/licenses/by-nc-sa/4.0/',
        'data_role': 'auxiliary_entity_ner_train_eval_not_original_ads',
        'notes': 'Preserve source train/validation/test split; never combine gold test with train; verify underlying source rights before redistribution.',
        'downloaded_at': now()}, ensure_ascii=False, indent=2), encoding='utf-8')
    output = REPORTS / 'ner_import_report.json'
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'NER report: {output.relative_to(ROOT)}')
    return report


def count_report():
    lookup = {
        'promotional_candidates_rss': RAW / 'posts_auto.jsonl',
        'evidence_discovery_rss': RAW / 'evidence_discovery.jsonl',
        'research_metadata_crossref': RAW / 'research_metadata.jsonl',
        'ner_train_external': EXTERNAL / 'train.jsonl',
        'ner_validation_external': EXTERNAL / 'validation.jsonl',
        'ner_test_external': EXTERNAL / 'test.jsonl',
    }
    result = {'run_at': now(), 'counts': {}, 'warning': 'These are DIFFERENT data roles. Do not add them up as original ads; only validated source ads count toward the ad dataset.'}
    for typ, path in lookup.items():
        rows = list(load_jsonl(path))
        result['counts'][typ] = len(rows)
        print(f'{typ:29s}: {len(rows)}')
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / 'dataset_roles_report.json'
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Report:', out.relative_to(ROOT))
    return result


def selftest():
    example = '\n<?xml version="1.0" encoding="utf-8"?><rss><channel><item><title>Ví dụ</title><link>https://example.org/a?utm_source=test</link><description><![CDATA[<b>Hỗ trợ</b> sức khỏe]]></description></item></channel></rss>'.encode('utf-8')
    parsed = parse_rss(example)
    assert len(parsed) == 1
    assert parsed[0]['summary'] == 'Hỗ trợ sức khỏe'
    assert canonical(parsed[0]['url']) == 'https://example.org/a'
    assert sha('a') == hashlib.sha256(b'a').hexdigest()
    print('Self-test PASS (synthetic fixture; no external data was harvested)')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['self-test', 'rss', 'ner', 'report', 'all'])
    p.add_argument('--max-per-feed', type=int, default=50)
    p.add_argument('--sleep-seconds', type=float, default=2.5)
    args = p.parse_args()
    if args.command == 'self-test':
        selftest()
    if args.command in ('ner', 'all'):
        download_ner()
    if args.command in ('rss', 'all'):
        collect_rss(limit=args.max_per_feed, sleep_seconds=args.sleep_seconds)
    if args.command in ('report', 'all'):
        count_report()


if __name__ == '__main__':
    main()
