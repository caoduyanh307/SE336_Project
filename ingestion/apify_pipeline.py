#!/usr/bin/env python3
"""Apify -> SE363 incremental ingestion. No Codex, MCP or third-party SDK required.

This is an *additional* source adapter, not the official Meta Threads API. It does
not change/disable ingestion.threads_collector or falsely label Apify as official.
Default commands only READ finished Actor runs, never launch a paid run.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from ingestion.threads_role_classifier import classify_post

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
VN = timezone(timedelta(hours=7))
ACTOR_ID = 'automation_craft~threads-scraper'
API = 'https://api.apify.com/v2'
RAW_REL = Path('data/raw/threads_posts_apify.jsonl')
DB_REL = Path('data/state/apify_pipeline.sqlite')
EXPORT_REL = Path('data/exports')
DATASET_COLS = ('post_id', 'permalink', 'raw_text', 'published_at', 'observed_at',
                'post_role', 'version', 'search_queries', 'apify_run_id',
                'collection_method', 'source_role', 'review_status')


def timestamp(value=None):
    value = value or datetime.now(UTC)
    return value.astimezone(UTC).isoformat(timespec='seconds')


def parse_datetime(value):
    if isinstance(value, datetime):
        d = value
    elif isinstance(value, str) and value:
        d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    else:
        raise ValueError('missing timestamp')
    if d.tzinfo is None:
        raise ValueError('missing timezone')
    return d.astimezone(UTC)


def load_settings(root=ROOT, environ=None):
    cfg = {}
    path = root / '.env'
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            if line.strip() and not line.lstrip().startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                cfg[k.strip()] = v.strip().strip('"').strip("'")
    cfg.update(os.environ if environ is None else environ)
    return cfg


def settings_int(cfg, key, default, lower, upper):
    try:
        val = int(cfg.get(key, default))
    except (TypeError, ValueError):
        raise ValueError(f'{key} must be an integer') from None
    if not lower <= val <= upper:
        raise ValueError(f'{key} outside {lower}..{upper}')
    return val


@dataclass
class Settings:
    token: str
    task_id: str
    collection_permission: str
    storage_permission: str
    reviewed_at: str
    retention_days: int
    run_scan_limit: int
    per_run_limit: int
    allow_paid: bool

    @classmethod
    def load(cls, root=ROOT, environ=None):
        cfg = load_settings(root, environ)
        obj = cls(
            token=str(cfg.get('APIFY_TOKEN', '')).strip(),
            task_id=str(cfg.get('APIFY_TASK_ID', '')).strip(),
            collection_permission=str(cfg.get('APIFY_COLLECTION_PERMISSION_STATUS', 'unverified')).strip(),
            storage_permission=str(cfg.get('APIFY_STORAGE_PERMISSION_STATUS', 'unverified')).strip(),
            reviewed_at=str(cfg.get('APIFY_POLICY_REVIEWED_AT', '')).strip(),
            retention_days=settings_int(cfg, 'APIFY_RETENTION_DAYS', 30, 1, 365),
            run_scan_limit=settings_int(cfg, 'APIFY_MAX_RUNS_SCAN', 1000, 1, 10000),
            per_run_limit=settings_int(cfg, 'APIFY_MAX_ITEMS_PER_RUN', 10000, 1, 100000),
            allow_paid=str(cfg.get('APIFY_ALLOW_PAID_RUN', '0')).strip() == '1',
        )
        return obj

    def check(self):
        if not self.token:
            raise RuntimeError('Missing APIFY_TOKEN in local .env')
        if self.collection_permission != 'verified':
            raise RuntimeError('Collection permissions are unverified; review Apify and Threads terms first')
        if self.storage_permission not in {'metadata_only', 'full_text'}:
            raise RuntimeError('Choose a reviewed storage policy: metadata_only or full_text')
        try:
            d = datetime.strptime(self.reviewed_at, '%Y-%m-%d').date()
            if d > datetime.now(UTC).date():
                raise ValueError
        except ValueError:
            raise RuntimeError('APIFY_POLICY_REVIEWED_AT must be a reviewed YYYY-MM-DD date') from None


class ApiError(RuntimeError):
    pass


class ApifyAPI:
    def __init__(self, token, transport=None, sleep=time.sleep):
        self.token = token
        self.transport = transport
        self.sleep = sleep

    def request(self, method, path, params=None, body=None):
        if not path.startswith('/') or not re.fullmatch(r'/[A-Za-z0-9_~/%.,=\-]+', path):
            raise ValueError('Unexpected Apify endpoint')
        url = API + path + (('?' + urlencode(params)) if params else '')
        payload = json.dumps(body).encode('utf-8') if body is not None else None
        req = Request(url, data=payload, method=method, headers={
            'Authorization': 'Bearer ' + self.token,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'SE363AcademicPipeline/1.0',
        })
        for attempt in range(3):
            try:
                if self.transport is None:
                    with urlopen(req, timeout=60) as response:
                        raw = response.read(16 * 1024 * 1024 + 1)
                else:
                    raw = self.transport(req)
                if len(raw) > 16 * 1024 * 1024:
                    raise ApiError('Apify response exceeds 16 MB; reduce page size')
                return json.loads(raw)
            except HTTPError as e:
                if e.code in {401, 403}:
                    raise ApiError(f'Apify HTTP {e.code}: check API token/permissions') from None
                if e.code == 429:
                    raise ApiError('Apify HTTP 429: rate limited, retry at a later scheduled run') from None
                if e.code not in {500, 502, 503, 504} or attempt == 2:
                    raise ApiError(f'Apify HTTP {e.code}') from None
                self.sleep(2 ** attempt)
            except (URLError, TimeoutError) as e:
                if attempt == 2:
                    raise ApiError('Apify network unavailable after retries') from None
                self.sleep(2 ** attempt)
            except json.JSONDecodeError:
                raise ApiError('Invalid JSON response from Apify') from None
        raise ApiError('Apify request failed')

    def run(self, run_id):
        if not re.fullmatch(r'[A-Za-z0-9]{10,50}', run_id):
            raise ValueError('Invalid run ID')
        data = self.request('GET', f'/actor-runs/{run_id}')
        if not isinstance(data, dict) or not isinstance(data.get('data'), dict):
            raise ApiError('Unexpected Apify run response')
        return data['data']

    def runs(self, task_id, max_runs):
        if not re.fullmatch(r'[A-Za-z0-9_~\-]{3,150}', task_id):
            raise ValueError('Invalid APIFY_TASK_ID')
        offset = 0
        while offset < max_runs:
            limit = min(100, max_runs - offset)
            data = self.request('GET', f'/actor-tasks/{task_id}/runs', {
                'limit': limit, 'offset': offset, 'desc': '0', 'status': 'SUCCEEDED'})
            page = data.get('data', {}) if isinstance(data, dict) else {}
            items = page.get('items') if isinstance(page, dict) else None
            if not isinstance(items, list):
                raise ApiError('Unexpected Apify run listing')
            for run in items:
                if isinstance(run, dict):
                    yield run
            if len(items) < limit:
                break
            offset += len(items)
        else:
            # A bounded scan must not silently pretend all historical runs were read.
            raise ApiError('APIFY_MAX_RUNS_SCAN reached; increase limit or sync more frequently')

    def items(self, dataset_id, max_items):
        if not re.fullmatch(r'[A-Za-z0-9]{10,50}', dataset_id):
            raise ValueError('Invalid dataset ID')
        offset = 0
        while offset < max_items:
            limit = min(250, max_items - offset)
            data = self.request('GET', f'/datasets/{dataset_id}/items', {
                'offset': offset, 'limit': limit, 'clean': '1'})
            if not isinstance(data, list):
                raise ApiError('Unexpected Apify dataset items response')
            for row in data:
                if isinstance(row, dict):
                    yield row
            if len(data) < limit:
                break
            offset += len(data)
        else:
            raise ApiError('APIFY_MAX_ITEMS_PER_RUN reached; increase limit for a full import')


def valid_post(item):
    if item.get('type') != 'post':
        return None
    post_id = str(item.get('postId') or item.get('post_id') or '')
    url = str(item.get('url') or item.get('permalink') or '')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,200}', post_id):
        return None
    try:
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.hostname not in {
            'threads.com', 'www.threads.com', 'threads.net', 'www.threads.net'
        } or parsed.username or parsed.password or parsed.port not in (None, 443):
            return None
    except ValueError:
        return None
    try:
        when = parse_datetime(item.get('postedAt') or item.get('timestamp') or item.get('posted_at'))
    except (TypeError, ValueError):
        return None
    body = item.get('text')
    if body is not None and not isinstance(body, str):
        return None
    keyword = item.get('sourceQuery') or item.get('keyword') or ''
    if not isinstance(keyword, str):
        keyword = ''
    return {
        'post_id': post_id,
        'permalink': parsed._replace(query='', fragment='').geturl(),
        'published_at': timestamp(when),
        'text': body or '',
        'media_type': str(item.get('mediaType') or ''),
        'keyword': keyword[:200],
    }


def sha(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def write_jsonl_append(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
        f.flush()
        os.fsync(f.fileno())


def atomic_write(path, writer):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.se363-')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as stream:
            writer(stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Store:
    def __init__(self, root, settings):
        self.root, self.settings = Path(root), settings
        self.path = self.root / DB_REL
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path)
        os.chmod(self.path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS posts (post_id TEXT PRIMARY KEY, latest_version INTEGER, last_seen TEXT);
        CREATE TABLE IF NOT EXISTS versions (post_id TEXT, version INTEGER, content_hash TEXT,
            record_json TEXT, exported INTEGER DEFAULT 0, PRIMARY KEY(post_id, version),
            UNIQUE(post_id, content_hash));
        CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, dataset_id TEXT, synced_at TEXT,
            accepted INTEGER, rejected INTEGER);
        CREATE TABLE IF NOT EXISTS keyword_hits (post_id TEXT, keyword TEXT, PRIMARY KEY(post_id, keyword));
        CREATE TABLE IF NOT EXISTS suppressions (post_id TEXT PRIMARY KEY, suppressed_at TEXT);
        ''')
        self.db.commit()
        self.noise = []
        file = self.root / 'config/threads_keywords.json'
        if file.exists():
            config = json.loads(file.read_text(encoding='utf-8'))
            self.noise = config.get('classification_only_noise_terms', [])
        self.seen_raw = set()
        path = self.root / RAW_REL
        if path.exists():
            with path.open(encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        self.seen_raw.add((str(data['post_id']), int(data['version'])))
                    except (ValueError, TypeError, KeyError):
                        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.db.close()

    def has_run(self, run_id):
        return self.db.execute('SELECT 1 FROM runs WHERE run_id=?', (run_id,)).fetchone() is not None

    def ingest(self, post, run_id):
        pid, body = post['post_id'], post['text']
        if self.db.execute('SELECT 1 FROM suppressions WHERE post_id=?', (pid,)).fetchone():
            return 'suppressed'
        digest = sha(json.dumps({'text': body, 'media_type': post['media_type']}, ensure_ascii=False, sort_keys=True))
        existing = self.db.execute('SELECT version FROM versions WHERE post_id=? AND content_hash=?', (pid, digest)).fetchone()
        now = timestamp()
        with self.db:
            if post['keyword']:
                self.db.execute('INSERT OR IGNORE INTO keyword_hits VALUES (?,?)', (pid, post['keyword']))
            if existing:
                self.db.execute('UPDATE posts SET last_seen=? WHERE post_id=?', (now, pid))
                return 'unchanged'
            old = self.db.execute('SELECT latest_version FROM posts WHERE post_id=?', (pid,)).fetchone()
            version = (old['latest_version'] if old else 0) + 1
            classification = classify_post(body, search_queries=[post['keyword']], noise_terms=self.noise)
            record = {
                'post_id': pid, 'permalink': post['permalink'],
                'published_at': post['published_at'], 'observed_at': now,
                'source_id': 'apify_threads', 'source_platform': 'threads',
                'source_role': 'threads_post_candidate',
                'source_type': 'third_party_public_search',
                'collection_method': 'apify_automation_craft_threads_scraper',
                'apify_run_id': run_id, 'search_queries': [post['keyword']] if post['keyword'] else [],
                'content_hash': digest, 'version': version,
                'media_type': post['media_type'],
                'retention_mode': 'full_text' if self.settings.storage_permission == 'full_text' else 'references_only',
                'collection_permission_status': self.settings.collection_permission,
                'storage_permission_status': self.settings.storage_permission,
                'redistribution_status': 'not_permitted',
                'policy_reviewed_at': self.settings.reviewed_at,
                'license_status': 'requires_platform_terms_review',
                'original_paid_ad_confirmed': False,
                'review_status': 'unreviewed',
                **classification,
            }
            if self.settings.storage_permission == 'full_text':
                record['raw_text'] = body
            self.db.execute('INSERT INTO versions (post_id,version,content_hash,record_json) VALUES (?,?,?,?)',
                            (pid, version, digest, json.dumps(record, ensure_ascii=False)))
            self.db.execute('INSERT INTO posts VALUES (?,?,?) ON CONFLICT(post_id) DO UPDATE SET latest_version=excluded.latest_version,last_seen=excluded.last_seen',
                            (pid, version, now))
        return 'new' if old is None else 'updated'

    def flush(self):
        """Recover SQLite->JSONL after a crash, without replaying committed events."""
        rows = self.db.execute('SELECT post_id,version,record_json FROM versions WHERE exported=0 ORDER BY rowid').fetchall()
        records = []
        events = {}
        for row in rows:
            identity = (row['post_id'], row['version'])
            record = json.loads(row['record_json'])
            if identity not in self.seen_raw:
                records.append(record)
                self.seen_raw.add(identity)
            event_id = sha(f'apify|{row["post_id"]}|{row["version"]}')
            day = parse_datetime(record['observed_at']).astimezone(VN).strftime('%Y%m%d')
            events.setdefault(day, []).append({
                'event_id': event_id,
                'event_type': 'threads.post.discovered' if row['version'] == 1 else 'threads.post.updated',
                'source_platform': 'threads', 'source_id': 'apify_threads',
                'collection_method': 'apify_automation_craft_threads_scraper',
                'post_id': row['post_id'], 'source_version': row['version'],
                'event_time': record['published_at'], 'ingestion_time': record['observed_at'],
                'payload': {'post_id': row['post_id'], 'permalink': record['permalink'],
                            'version': row['version'], 'content_hash': record['content_hash'],
                            'source_role': record['source_role']},
            })
        if records:
            write_jsonl_append(self.root / RAW_REL, records)
        for day, new_events in events.items():
            target = self.root / f'data/events/apify_threads_events_{day}.jsonl'
            known = set()
            if target.exists():
                with target.open(encoding='utf-8') as f:
                    for line in f:
                        try:
                            known.add(json.loads(line)['event_id'])
                        except (ValueError, KeyError):
                            pass
            write_jsonl_append(target, [e for e in new_events if e['event_id'] not in known])
        with self.db:
            for row in rows:
                self.db.execute('UPDATE versions SET exported=1 WHERE post_id=? AND version=?',
                                (row['post_id'], row['version']))
        return len(records)

    def mark_run(self, run_id, dataset_id, accepted, rejected):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?)',
                            (run_id, dataset_id, timestamp(), accepted, rejected))

    def report(self):
        return {
            'unique_posts': self.db.execute('SELECT COUNT(*) FROM posts WHERE post_id NOT IN (SELECT post_id FROM suppressions)').fetchone()[0],
            'versions': self.db.execute('SELECT COUNT(*) FROM versions WHERE post_id NOT IN (SELECT post_id FROM suppressions)').fetchone()[0],
            'runs_synced': self.db.execute('SELECT COUNT(*) FROM runs').fetchone()[0],
            'suppressed_posts': self.db.execute('SELECT COUNT(*) FROM suppressions').fetchone()[0],
        }

    def export(self):
        rows = self.db.execute('''SELECT v.record_json FROM versions v JOIN posts p
            ON p.post_id=v.post_id AND p.latest_version=v.version
            WHERE v.post_id NOT IN (SELECT post_id FROM suppressions) ORDER BY v.post_id''').fetchall()
        records = []
        cutoff = datetime.now(UTC) - timedelta(days=self.settings.retention_days)
        for row in rows:
            record = json.loads(row[0])
            try:
                if parse_datetime(record['observed_at']) < cutoff:
                    continue
            except (KeyError, ValueError):
                continue
            record['search_queries'] = [r[0] for r in self.db.execute(
                'SELECT keyword FROM keyword_hits WHERE post_id=? ORDER BY keyword', (record['post_id'],))]
            records.append(record)
        base = self.root / EXPORT_REL
        atomic_write(base / 'apify_threads_latest.json', lambda f: json.dump(records, f, ensure_ascii=False, indent=2))
        def csv_writer(f):
            writer = csv.DictWriter(f, fieldnames=DATASET_COLS)
            writer.writeheader()
            for row in records:
                writer.writerow({col: ', '.join(row[col]) if col == 'search_queries' else row.get(col, '') for col in DATASET_COLS})
        atomic_write(base / 'apify_threads_latest.csv', csv_writer)
        return len(records)

    def suppress(self, post_id):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,200}', post_id):
            raise ValueError('Invalid post ID')
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO suppressions VALUES (?,?)', (post_id, timestamp()))
            self.db.execute('DELETE FROM versions WHERE post_id=?', (post_id,))
            self.db.execute('DELETE FROM posts WHERE post_id=?', (post_id,))
            self.db.execute('DELETE FROM keyword_hits WHERE post_id=?', (post_id,))
        # Remove suppressed content from the local managed append-only raw file.
        raw = self.root / RAW_REL
        if raw.exists():
            def rewrite(f):
                for line in raw.read_text(encoding='utf-8').splitlines():
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if str(row.get('post_id')) != post_id:
                        f.write(json.dumps(row, ensure_ascii=False) + '\n')
            atomic_write(raw, rewrite)
        self.seen_raw = {key for key in self.seen_raw if key[0] != post_id}
        # Event spool contains references only, but their URLs can still identify a user.
        for path in (self.root / 'data/events').glob('apify_threads_events_*.jsonl'):
            def rewrite_events(f):
                for line in path.read_text(encoding='utf-8').splitlines():
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if str(row.get('post_id')) != post_id:
                        f.write(json.dumps(row, ensure_ascii=False) + '\n')
            atomic_write(path, rewrite_events)
        # Existing claim-review exports may still contain the suppressed text.
        for path in (self.root / 'data/auto_baseline/apify_threads').glob('*.csv'):
            try:
                with path.open(encoding='utf-8-sig', newline='') as f:
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames or []
                    records = [row for row in reader if row.get('post_id') != post_id]
                if 'post_id' in headers:
                    def save(f):
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(records)
                    atomic_write(path, save)
            except (UnicodeDecodeError, OSError):
                raise RuntimeError('Could not clean claim-review output after suppression') from None
        self.export()

    def purge_expired(self):
        """Enforce local research retention in managed raw/state/events/exports."""
        cutoff = datetime.now(UTC) - timedelta(days=self.settings.retention_days)
        to_delete = []
        for row in self.db.execute('SELECT post_id,version,record_json FROM versions').fetchall():
            record = json.loads(row['record_json'])
            try:
                observed = parse_datetime(record['observed_at'])
            except (KeyError, ValueError):
                observed = datetime(1970, 1, 1, tzinfo=UTC)
            if observed < cutoff:
                to_delete.append((row['post_id'], row['version']))
        if not to_delete:
            return 0
        with self.db:
            for pid, version in to_delete:
                self.db.execute('DELETE FROM versions WHERE post_id=? AND version=?', (pid, version))
            for pid, in self.db.execute('SELECT post_id FROM posts').fetchall():
                row = self.db.execute('SELECT MAX(version) FROM versions WHERE post_id=?', (pid,)).fetchone()
                if row[0] is None:
                    self.db.execute('DELETE FROM posts WHERE post_id=?', (pid,))
                    self.db.execute('DELETE FROM keyword_hits WHERE post_id=?', (pid,))
                else:
                    self.db.execute('UPDATE posts SET latest_version=? WHERE post_id=?', (row[0], pid))
        removed = set(to_delete)
        raw = self.root / RAW_REL
        if raw.exists():
            def rewrite_raw(f):
                for line in raw.read_text(encoding='utf-8').splitlines():
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    if (str(record.get('post_id')), int(record.get('version', 0))) not in removed:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
            atomic_write(raw, rewrite_raw)
        for path in (self.root / 'data/events').glob('apify_threads_events_*.jsonl'):
            def rewrite_events(f):
                for line in path.read_text(encoding='utf-8').splitlines():
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if (str(row.get('post_id')), int(row.get('source_version', 0))) not in removed:
                        f.write(json.dumps(row, ensure_ascii=False) + '\n')
            atomic_write(path, rewrite_events)
        self.seen_raw.difference_update(removed)
        self.export()
        return len(removed)


def process_run(api, store, run):
    run_id, dataset_id = str(run.get('id', '')), str(run.get('defaultDatasetId', ''))
    if not re.fullmatch(r'[A-Za-z0-9]{10,50}', run_id) or not dataset_id:
        raise ApiError('Malformed successful run metadata')
    if run.get('status') != 'SUCCEEDED':
        raise ApiError(f'Run {run_id} status is not SUCCEEDED')
    if store.has_run(run_id):
        return {'run_id': run_id, 'status': 'already_synced'}
    accepted = rejected = new = updated = unchanged = 0
    for item in api.items(dataset_id, store.settings.per_run_limit):
        parsed = valid_post(item)
        if not parsed:
            rejected += 1
            continue
        accepted += 1
        outcome = store.ingest(parsed, run_id)
        if outcome in {'new', 'updated', 'unchanged'}:
            if outcome == 'new':
                new += 1
            elif outcome == 'updated':
                updated += 1
            else:
                unchanged += 1
    store.flush()
    store.mark_run(run_id, dataset_id, accepted, rejected)
    return {'run_id': run_id, 'status': 'synced', 'accepted': accepted,
            'ignored_rows': rejected, 'new': new, 'updated': updated,
            'unchanged': unchanged}


def atomic_report(root, result):
    def write(f):
        json.dump(result, f, ensure_ascii=False, indent=2)
    atomic_write(root / 'data/reports/apify_sync_latest.json', write)


def run(root=ROOT, argv=None, api=None, config=None):
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('audit')
    sub.add_parser('status')
    sub.add_parser('purge', help='Remove locally expired Apify snapshots and events')
    sub.add_parser('sync')
    r = sub.add_parser('recover', help='Import an existing SUCCEEDED run without charging a new scrape')
    r.add_argument('run_id')
    s = sub.add_parser('suppress', help='Delete a post from managed Apify dataset and block reimport')
    s.add_argument('post_id')
    a = p.parse_args(argv)
    config = config or Settings.load(root)
    if a.command == 'audit':
        result = {'token_present': bool(config.token), 'task_configured': bool(config.task_id),
                  'collection_permission_status': config.collection_permission,
                  'storage_permission_status': config.storage_permission,
                  'retention_days': config.retention_days, 'reviewed_at': config.reviewed_at}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result
    if a.command in {'sync', 'recover'}:
        config.check()
    if a.command == 'sync' and not config.task_id:
        raise RuntimeError('APIFY_TASK_ID missing: create an Apify Task+Schedule, or recover <run_id>')
    lock = root / 'data/state/apify_pipeline.lock'
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a+') as lockfile:
        fcntl.flock(lockfile, fcntl.LOCK_EX)
        with Store(root, config) as store:
            store.flush()  # crash recovery
            if a.command == 'status':
                result = store.report()
            elif a.command == 'suppress':
                store.suppress(a.post_id)
                result = {'suppressed': a.post_id, **store.report()}
            elif a.command == 'purge':
                removed = store.purge_expired()
                result = {'versions_purged': removed, **store.report()}
            else:
                api = api or ApifyAPI(config.token)
                attempts = []
                if a.command == 'recover':
                    attempts = [process_run(api, store, api.run(a.run_id))]
                else:
                    for summary in api.runs(config.task_id, config.run_scan_limit):
                        if not store.has_run(str(summary.get('id', ''))):
                            attempts.append(process_run(api, store, api.run(summary['id'])))
                expired = store.purge_expired()
                exported = store.export()
                result = {'status': 'OK', 'versions_purged': expired, 'runs': attempts, 'latest_posts_exported': exported, **store.report()}
                atomic_report(root, result)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return result


def main():
    try:
        run()
    except (ApiError, RuntimeError, OSError, sqlite3.Error, ValueError) as exc:
        # Do not print HTTP response bodies, bearer tokens or private post contents.
        print(f'[APIFY ERROR] {exc}', file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
