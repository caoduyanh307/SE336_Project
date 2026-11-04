#!/usr/bin/env python3
"""SE363: optional RSS discovery and local dataset role counts.

Modes:
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
USER_AGENT = 'SE363-AcademicDataCollector/1.0 (RSS summaries only; noncommercial research)'


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


PUBLISHER_RSS_TYPES = {'publisher_store_rss_candidate', 'publisher_rss_evidence_index'}


def rss_identity(row):
    """A publisher RSS item keeps its identity across feed/URL tracking changes."""
    role = str(row.get('source_role') or '')
    url = str(row.get('source_url') or row.get('url') or '')
    return 'rss:' + sha(role + '|' + canonical(url))


def rss_content_hash(row):
    # Publication/observation times and the feed category are not content edits.
    content = {'title': str(row.get('title') or ''),
               'summary': str(row.get('raw_text') or row.get('summary') or '')}
    return sha(json.dumps(content, ensure_ascii=False, sort_keys=True))


def rss_version_index(rows):
    """Infer legacy snapshots without changing their bytes or replaying them.

    A previously seen content hash is not appended again (including A->B->A).
    Versions therefore describe novel observed snapshots, not a complete edit log.
    """
    result = {}
    for row in rows:
        if row.get('source_type') not in PUBLISHER_RSS_TYPES:
            continue
        identity = rss_identity(row)
        state = result.setdefault(identity, {'hashes': {}, 'version': 0, 'post_id': None})
        content_hash = rss_content_hash(row)
        if content_hash not in state['hashes']:
            try:
                version = max(state['version'] + 1, int(row.get('version') or 1))
            except (TypeError, ValueError):
                version = state['version'] + 1
            state['hashes'][content_hash] = version
            state['version'] = version
        if row.get('post_id'):
            state['post_id'] = row['post_id']
    return result


def new_rss_versions(rows, index):
    """Prepare only novel publisher RSS snapshots for the canonical collector."""
    result = []
    for original in rows:
        identity = rss_identity(original)
        content_hash = rss_content_hash(original)
        state = index.setdefault(identity, {'hashes': {}, 'version': 0, 'post_id': None})
        if content_hash in state['hashes']:
            continue
        version = state['version'] + 1
        row = dict(original, source_record_id=identity, version=version,
                   content_hash_sha256=content_hash,
                   _key='rss_snapshot:' + sha(identity + '|' + content_hash))
        if row.get('source_role') == 'promotional_content_candidate':
            row['post_id'] = state['post_id'] or 'RSS-' + sha(canonical(str(row.get('source_url') or row.get('url') or '')))[:12]
            state['post_id'] = row['post_id']
        state['hashes'][content_hash] = version
        state['version'] = version
        result.append(row)
    return result


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
    indexes = {}
    for key, dest in [('promotion_candidate', RAW / 'posts_auto.jsonl'),
                      ('evidence_candidate', RAW / 'evidence_discovery.jsonl')]:
        indexes[key] = rss_version_index(load_jsonl(dest))
    report = {'run_at': now(), 'mode': 'rss', 'limits': 'RSS summary/title only; noncommercial use; NOT original ad verification; NOT independently confirmed evidence', 'feeds': [], 'errors': []}
    touched = set()
    for idx, feed in enumerate(feeds):
        if idx:
            time.sleep(max(0.0, sleep_seconds))
        role = feed.get('role', '')
        url = feed.get('url', '')
        if not feed.get('authorized_for_collection') or role not in indexes or not url.startswith('https://'):
            report['feeds'].append({'url': url, 'skipped': True, 'reason': 'rights/role/url not approved'})
            continue
        try:
            data = request_bytes(url, max_bytes=2 * 1024 * 1024)
            items = parse_rss(data)
            out = RAW / ('posts_auto.jsonl' if role == 'promotion_candidate' else 'evidence_discovery.jsonl')
            newrows = []
            for item in items[:limit]:
                norm_url = canonical(item['url'])
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
            newrows = new_rss_versions(newrows, indexes[role])
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


def count_report():
    raw_files = ('posts_auto.jsonl', 'evidence_discovery.jsonl', 'research_metadata.jsonl',
                 'product_metadata.jsonl', 'threads_posts.jsonl', 'threads_post_refs.jsonl')
    result = {'run_at': now(), 'counts': {}, 'raw_rows_by_file': {},
              'by_role': {}, 'by_source_type': {},
              'warning': 'Roles and source types come from records, not filenames. RSS counts are unique source records; raw_rows include content versions. Candidates are not verified ads or gold labels.'}
    identities_by_role = {}
    identities_by_source = {}
    identities = set()
    historic = {'promotional_candidates_rss': set(), 'evidence_discovery_rss': set(),
                'research_metadata_crossref': set()}
    for name in raw_files:
        rows = list(load_jsonl(RAW / name))
        result['raw_rows_by_file'][name] = len(rows)
        for i, row in enumerate(rows):
            role = str(row.get('source_role') or 'unknown')
            source_type = str(row.get('source_type') or row.get('source_platform') or 'unknown')
            identity = (role, source_type, str(row.get('source_record_id') or row.get('post_id')
                        or row.get('doi') or row.get('product_id')
                        or row.get('source_url') or row.get('url') or row.get('_key') or f'{name}:{i}'))
            # Old publisher rows have no source_record_id; apply the same canonical
            # identity used for new snapshots without modifying the source file.
            if source_type in PUBLISHER_RSS_TYPES:
                identity = (role, source_type, rss_identity(row))
            identities.add(identity)
            for key, bucket, known in ((role, result['by_role'], identities_by_role),
                                       (source_type, result['by_source_type'], identities_by_source)):
                bucket.setdefault(key, {'raw_rows': 0, 'unique_records': 0})['raw_rows'] += 1
                known.setdefault(key, set()).add(identity)
            if role == 'promotional_content_candidate' and source_type in {'publisher_store_rss_candidate', 'approved_brand_rss'}:
                historic['promotional_candidates_rss'].add(identity)
            elif role == 'evidence_candidate' and source_type == 'publisher_rss_evidence_index':
                historic['evidence_discovery_rss'].add(identity)
            elif role == 'research_metadata' and source_type == 'crossref':
                historic['research_metadata_crossref'].add(identity)
    for key, values in historic.items():
        result['counts'][key] = len(values)
    for bucket, known in ((result['by_role'], identities_by_role),
                          (result['by_source_type'], identities_by_source)):
        for key, values in known.items():
            bucket[key]['unique_records'] = len(values)
    result['totals'] = {'raw_rows': sum(result['raw_rows_by_file'].values()),
                        'unique_records': len(identities)}
    lookup = {
        'ner_train_external': EXTERNAL / 'train.jsonl',
        'ner_validation_external': EXTERNAL / 'validation.jsonl',
        'ner_test_external': EXTERNAL / 'test.jsonl',
    }
    for typ, path in lookup.items():
        rows = list(load_jsonl(path))
        result['counts'][typ] = len(rows)
    for typ, count in result['counts'].items():
        print(f'{typ:29s}: {count}')
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
    p.add_argument('command', choices=['self-test', 'rss', 'report'])
    p.add_argument('--max-per-feed', type=int, default=50)
    p.add_argument('--sleep-seconds', type=float, default=2.5)
    args = p.parse_args()
    if args.command == 'self-test':
        selftest()
    if args.command == 'rss':
        collect_rss(limit=args.max_per_feed, sleep_seconds=args.sleep_seconds)
    if args.command == 'report':
        count_report()


if __name__ == '__main__':
    main()
