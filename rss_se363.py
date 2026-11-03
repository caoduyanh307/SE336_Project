#!/usr/bin/env python3
"""SE363: RSS discovery from explicitly listed publisher feeds.

Only RSS titles / supplied summaries / links are recorded. No crawling article
pages, no login, no CAPTCHA bypass, no commercial redistribution. Publisher
materials are not treated as verified evidence or original paid advertising.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
from html import unescape
from html.parser import HTMLParser
import json
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / 'sources' / 'approved_rss_feeds.json'
RAW = ROOT / 'data' / 'raw'
REPORT_DIR = ROOT / 'data' / 'reports'

class TextOnly(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
    def handle_data(self, data):
        self.parts.append(data)

def plain(value):
    p = TextOnly()
    p.feed(unescape(value or ''))
    return ' '.join(' '.join(p.parts).split())

def sha(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()

def iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

def local(tag):
    return tag.rsplit('}', 1)[-1]

def first_text(elem, *names):
    for name in names:
        for child in elem:
            if local(child.tag) == name:
                return ''.join(child.itertext()).strip()
    return ''

def parse_items(xml):
    root = ET.fromstring(xml.lstrip())
    rows = []
    for item in root.iter():
        if local(item.tag) not in ('entry', 'item'):
            continue
        url = first_text(item, 'link')
        if not url:
            for child in item:
                if local(child.tag) == 'link':
                    url = child.get('href', '')
                    if url:
                        break
        title = plain(first_text(item, 'title'))
        summary = plain(first_text(item, 'description', 'summary', 'content', 'encoded'))
        if url.startswith('http') and (title or summary):
            rows.append({
                'url': url, 'title': title, 'summary': summary,
                'published_at': first_text(item, 'pubDate', 'published', 'updated')
            })
    return rows

def seen_keys(path):
    keys = set()
    if not path.exists():
        return keys
    with path.open(encoding='utf-8') as f:
        for line in f:
            try:
                keys.add(json.loads(line)['_key'])
            except (ValueError, KeyError):
                pass
    return keys

def append_unique(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    known = seen_keys(path)
    added = 0
    with path.open('a', encoding='utf-8') as f:
        for row in rows:
            key = row['_key']
            if key in known:
                continue
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
            added += 1
            known.add(key)
    return added

def export_csv(path):
    if not path.exists():
        return
    with path.open(encoding='utf-8') as f:
        rows = [json.loads(line) for line in f if line.strip()]
    if not rows:
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with path.with_suffix('.csv').open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

def download_rss(url, contact_email=''):
    ua = 'SE363-StudentRSSCollector/1.0 (noncommercial educational research; RSS only)'
    if contact_email and '@' in contact_email:
        ua += ' Contact: ' + contact_email
    request = Request(url, headers={'User-Agent': ua, 'Accept': 'application/rss+xml,application/atom+xml,text/xml,application/xml'})
    with urlopen(request, timeout=25) as r:
        data = r.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise ValueError('RSS > 2MiB; bỏ qua nguồn')
    return data

def make_rows(feed, items, limit):
    role = feed['role']
    publisher = feed['publisher']
    observed = iso()
    output = []
    for item in items[:limit]:
        # Deduplicate on URL + its current RSS summary, not on run time.
        key = 'se363rss:' + sha(role + '|' + item['url'] + '|' + item['summary'])
        core = {
            '_key': key,
            'source_role': ('evidence_candidate' if role == 'evidence_candidate' else 'promotional_content_candidate'),
            'source_type': ('publisher_rss_evidence_index' if role == 'evidence_candidate' else 'publisher_store_rss_candidate'),
            'publisher': publisher, 'title': item['title'],
            'source_url': item['url'], 'url': item['url'],
            'published_at': item['published_at'], 'observed_at': observed,
            'rss_feed_url': feed['url'], 'usage_note': 'RSS title/summary only; cite publisher; noncommercial/personal research',
            'terms_url': feed['terms_url'],
            'record_status': 'needs_review',
        }
        if role == 'evidence_candidate':
            core['summary'] = item['summary']
            core['note'] = 'Third-party news index; NOT original advertising, NOT proof of claim truth.'
        else:
            core['post_id'] = 'RSS-' + sha(item['url'])[:12]
            core['platform'] = 'publisher_website_rss'
            core['advertiser'] = ''
            core['raw_text'] = item['summary']
            core['content_hash_sha256'] = sha(item['summary'])
            core['note'] = 'Store category: only promotional candidate; speaker/advertiser and claim origin unverified.'
        output.append(core)
    return output

def self_test():
    sample = b'''<rss version="2.0"><channel><title>demo</title><item><title>Demo product</title><link>https://example.org/demo</link><description><![CDATA[<p>Ch\xe1\xbb\xa9a vitamin C</p>]]></description><pubDate>Mon, 02 Nov 2026 00:00:00 GMT</pubDate></item></channel></rss>'''
    assert len(parse_items(sample)) == 1
    assert parse_items(sample)[0]['url'] == 'https://example.org/demo'
    rows = make_rows({'role':'promotion_candidate','publisher':'Test','url':'https://example.org/feed','terms_url':'https://example.org/terms'}, parse_items(sample), 30)
    assert rows[0]['source_role'] == 'promotional_content_candidate'
    assert rows[0]['record_status'] == 'needs_review'
    print('Self-test PASS (dữ liệu giả chỉ dùng trong kiểm thử, không lưu vào dataset)')

def main():
    ap = argparse.ArgumentParser(description='RSS SE363: evidence discovery + promotional candidates')
    ap.add_argument('--self-test', action='store_true')
    ap.add_argument('--contact-email', default='', help='Email nghiên cứu (tùy chọn), chỉ đưa vào User-Agent')
    ap.add_argument('--max-per-feed', type=int, default=30)
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return
    if not CONFIG.exists():
        print('Thiếu cấu hình:', CONFIG, file=sys.stderr)
        sys.exit(1)
    feeds = json.loads(CONFIG.read_text(encoding='utf-8'))
    max_each = max(1, min(args.max_per_feed, 50))
    report = {'run_at': iso(), 'source': 'publisher RSS feeds', 'results': [], 'errors': [], 'limits': 'Feed summaries only; not original ads nor independently verified evidence'}
    targets = set()
    for ix, feed in enumerate(feeds):
        if ix:
            time.sleep(2)   # gentle spacing, no repeat loops
        if not feed.get('authorized_for_collection') or feed.get('role') not in ('evidence_candidate', 'promotion_candidate'):
            print('[SKIP] Chưa xác nhận quyền thu thập / role không hợp lệ')
            continue
        url = feed.get('url', '')
        if urlparse(url).scheme != 'https' or not urlparse(url).netloc:
            print('[SKIP] URL feed không hợp lệ')
            continue
        dest = RAW / ('evidence_discovery.jsonl' if feed['role'] == 'evidence_candidate' else 'posts_auto.jsonl')
        try:
            data = download_rss(url, args.contact_email)
            items = parse_items(data)
            rows = make_rows(feed, items, max_each)
            added = append_unique(dest, rows)
            targets.add(dest)
            report['results'].append({'feed': url, 'found': len(items), 'new': added, 'target': str(dest.relative_to(ROOT))})
            print(f'[RSS] {url} => found={len(items)} | new={added} | {dest.relative_to(ROOT)}')
        except HTTPError as e:
            reason = f'HTTP {e.code}'
            if e.code == 429:
                reason += ' - rate limited; dừng, KHÔNG thử lại liên tục'
            elif e.code == 403:
                reason += ' - không có quyền truy cập; không vượt rào'
            print('[RSS ERROR]', url, reason)
            report['errors'].append({'feed':url,'error':reason})
        except (OSError, URLError, ET.ParseError, ValueError) as e:
            print('[RSS ERROR]', url, str(e)[:200])
            report['errors'].append({'feed':url,'error':str(e)[:350]})
    for dest in targets:
        export_csv(dest)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output = REPORT_DIR / 'rss_ingestion_report.json'
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Report:', output.relative_to(ROOT))

if __name__ == '__main__':
    main()
