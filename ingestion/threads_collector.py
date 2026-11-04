"""Official Threads keyword ingestion, durable outbox, and offline-safe CLI.

JSONL and SQLite do not share a transaction. Commit an outbox with each content
version, fsync complete JSONL lines, then mark delivery. Recovery checks stable
event IDs before appending. This is at-least-once delivery with idempotent IDs,
not an exactly-once claim. No API response body, bearer, or post text is logged.
"""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .threads_role_classifier import classify_post

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
VN = timezone(timedelta(hours=7))
API_MIN_TIME = 1688540400
SAFE_FIELDS = {"id", "text", "timestamp", "permalink", "media_type"}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


# A separate name makes the transport patchable in offline tests. Never forward
# Authorization on a server-controlled redirect or a paging.next URL.
urlopen = build_opener(NoRedirect()).open


def stamp(value=None):
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat(timespec="seconds")


def parse_time(value):
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass
class Settings:
    token: str = field(default="", repr=False)
    enable_daily: bool = False
    api_base: str = "https://graph.threads.com"
    fields: str = "id,text,timestamp,permalink,media_type"
    search_type: str = "RECENT"
    search_mode: str = "KEYWORD"
    max_pages_per_keyword: int = 2
    page_limit: int = 25
    request_interval: float = 4.0
    lookback_hours: int = 48
    daily_max_requests: int = 30
    max_backfill_days: int = 7
    collection_permission_status: str = "unverified"
    storage_permission_status: str = "unverified"
    redistribution_status: str = "not_permitted"
    verified_scopes: tuple = ()
    policy_reviewed_at: str = ""
    source_terms_url: str = "https://developers.facebook.com/terms/"
    retention_days: int = 30

    @classmethod
    def from_env(cls, root=ROOT, environ=None):
        values = {}
        # Read locally; never change the environment or credential files.
        dotenv = Path(root) / ".env"
        if dotenv.exists():
            for line in dotenv.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip().strip('"').strip("'")
        values.update(os.environ if environ is None else environ)
        def get(key, default=""):
            return str(values.get(key, default)).strip()
        def integer(key, default, low, high):
            try:
                value = int(get(key, default))
            except ValueError:
                raise ValueError(f"Invalid numeric configuration: {key}") from None
            if not low <= value <= high:
                raise ValueError(f"Out-of-range configuration: {key}")
            return value
        try:
            interval = float(get("THREADS_REQUEST_INTERVAL_SECONDS", 4))
        except ValueError:
            raise ValueError("Invalid THREADS_REQUEST_INTERVAL_SECONDS") from None
        if not 4 <= interval <= 3600:
            raise ValueError("THREADS_REQUEST_INTERVAL_SECONDS must be 4..3600")
        result = cls(
            token=get("THREADS_ACCESS_TOKEN"),
            enable_daily=get("SE363_ENABLE_THREADS_DAILY", "0") == "1",
            api_base=get("THREADS_API_BASE", cls.api_base).rstrip("/"),
            fields=get("THREADS_SEARCH_FIELDS", cls.fields),
            search_type=get("THREADS_SEARCH_TYPE", "RECENT"),
            search_mode=get("THREADS_SEARCH_MODE", "KEYWORD"),
            max_pages_per_keyword=integer("THREADS_MAX_PAGES_PER_KEYWORD", 2, 1, 100),
            page_limit=integer("THREADS_PAGE_LIMIT", 25, 1, 100),
            request_interval=interval,
            lookback_hours=integer("THREADS_LOOKBACK_HOURS", 48, 1, 720),
            daily_max_requests=integer("THREADS_DAILY_MAX_REQUESTS", 30, 1, 1000),
            max_backfill_days=integer("THREADS_MAX_BACKFILL_DAYS", 7, 1, 30),
            collection_permission_status=get("THREADS_COLLECTION_PERMISSION_STATUS", "unverified"),
            storage_permission_status=get("THREADS_STORAGE_PERMISSION_STATUS", "unverified"),
            redistribution_status=get("THREADS_REDISTRIBUTION_STATUS", "not_permitted"),
            verified_scopes=tuple(filter(None, re.split(r"[,\s]+", get("THREADS_VERIFIED_SCOPES")))),
            policy_reviewed_at=get("THREADS_POLICY_REVIEWED_AT"),
            source_terms_url=get("THREADS_SOURCE_TERMS_URL", cls.source_terms_url),
            retention_days=integer("THREADS_RETENTION_DAYS", 30, 1, 365),
        )
        result.validate()
        return result

    def validate(self):
        if self.api_base not in {"https://graph.threads.net", "https://graph.threads.com"}:
            raise ValueError("THREADS_API_BASE must be an official HTTPS API origin")
        fields = set(self.fields.split(","))
        if not {"id", "timestamp", "permalink", "text"} <= fields or fields - SAFE_FIELDS:
            raise ValueError("THREADS_SEARCH_FIELDS must use the minimal supported field set")
        if self.search_type != "RECENT" or self.search_mode not in {"KEYWORD", "TAG"}:
            raise ValueError("Only RECENT and KEYWORD/TAG are supported")


class CollectorError(RuntimeError):
    def __init__(self, status, message, retry_after=None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


def retry_after_seconds(value):
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            return max(0.0, (parsedate_to_datetime(value).astimezone(UTC) - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def validate_response(obj):
    if not isinstance(obj, dict) or not isinstance(obj.get("data"), list):
        raise CollectorError("API_CONTRACT_ERROR", "Expected API data array; response body withheld")
    if any(not isinstance(row, dict) for row in obj["data"]):
        raise CollectorError("API_CONTRACT_ERROR", "Expected API record objects")
    paging = obj.get("paging", {})
    if not isinstance(paging, dict) or not isinstance(paging.get("cursors", {}), dict):
        raise CollectorError("API_CONTRACT_ERROR", "Invalid paging structure")
    return obj


class ThreadsClient:
    def __init__(self, settings, sleep=time.sleep, random_fn=random.random):
        settings.validate()
        self.settings, self.sleep, self.random_fn = settings, sleep, random_fn
        self.before_request = None
        self.requests = 0

    def search(self, params):
        if "access_token" in params:
            raise CollectorError("CONFIG_ERROR", "Credentials must not be URL parameters")
        req = Request(self.settings.api_base + "/keyword_search?" + urlencode(params), headers={
            "Authorization": "Bearer " + self.settings.token,
            "User-Agent": "SE363ThreadsResearch/1.0",
            "Accept": "application/json",
        })
        for attempt in range(3):
            if self.before_request:
                self.before_request()
            self.requests += 1
            try:
                with urlopen(req, timeout=30) as response:
                    data = response.read(8 * 1024 * 1024 + 1)
                if len(data) > 8 * 1024 * 1024:
                    raise CollectorError("API_CONTRACT_ERROR", "Response exceeds local size cap")
                try:
                    obj = json.loads(data)
                except (ValueError, UnicodeDecodeError):
                    raise CollectorError("API_CONTRACT_ERROR", "Invalid JSON response; body withheld") from None
                if isinstance(obj, dict) and "error" in obj:
                    code = obj["error"].get("code") if isinstance(obj["error"], dict) else None
                    status = "BLOCKED_PERMISSION" if code in {10, 190, 200} else "API_ERROR"
                    raise CollectorError(status, "API reported an error; body withheld")
                return validate_response(obj)
            except HTTPError as exc:
                if exc.code in {401, 403}:
                    raise CollectorError("BLOCKED_PERMISSION", f"HTTP {exc.code}; stop and review app authorization") from None
                if exc.code == 429:
                    wait = retry_after_seconds(exc.headers.get("Retry-After") if exc.headers else None)
                    raise CollectorError("THROTTLED", "HTTP 429; collection stopped", wait if wait is not None else 900) from None
                if exc.code == 400:
                    # Graph APIs can report expired OAuth grants as HTTP 400.
                    # Inspect only the numeric code; never echo the message/body.
                    try:
                        error = json.loads(exc.read(65536)).get("error", {})
                        permission_error = isinstance(error, dict) and error.get("code") in {10, 190, 200}
                    except (ValueError, AttributeError, UnicodeDecodeError):
                        permission_error = False
                    if permission_error:
                        raise CollectorError("BLOCKED_PERMISSION", "API token or permission rejected (HTTP 400)") from None
                if not 500 <= exc.code <= 599 or attempt == 2:
                    raise CollectorError("HTTP_ERROR", f"HTTP {exc.code}; response body withheld") from None
                wait = retry_after_seconds(exc.headers.get("Retry-After") if exc.headers else None)
                # Long server cooldowns are persisted by the caller, not slept through.
                if wait is not None and wait > 60:
                    raise CollectorError("THROTTLED", "Server requested a long retry delay", wait) from None
                self.sleep(max(wait or 0, 2 ** attempt + self.random_fn()))
            except (URLError, TimeoutError, OSError):
                if attempt == 2:
                    raise CollectorError("NETWORK_UNAVAILABLE", "Network retries exhausted; details withheld") from None
                self.sleep(2 ** attempt + self.random_fn())
        raise CollectorError("NETWORK_UNAVAILABLE", "Network retries exhausted")


def audit(root=ROOT, settings=None, now=None):
    try:
        settings = settings or Settings.from_env(root)
        settings.validate()
    except ValueError:
        return {"status": "CONFIG_ERROR", "fetched": 0, "network_called": False,
                "reason": "Invalid Threads configuration; check template bounds and official host"}
    status, reason = "READY", "Local declarations ready; no authenticated API verification performed"
    if not settings.token or settings.collection_permission_status != "verified" or not {
        "threads_basic", "threads_keyword_search"
    } <= set(settings.verified_scopes):
        status, reason = "BLOCKED_PERMISSION", "Token, verified collection permission and required scopes are needed"
    elif settings.storage_permission_status not in {"metadata_only", "full_text"}:
        status, reason = "BLOCKED_POLICY", "Explicit metadata or full-text storage permission is needed"
    else:
        try:
            reviewed = parse_time(settings.policy_reviewed_at)
            if reviewed > (now or datetime.now(UTC)) or not settings.source_terms_url.startswith("https://"):
                raise ValueError
        except ValueError:
            status, reason = "BLOCKED_POLICY", "A valid policy review date and HTTPS terms URL are needed"
    return {"status": status, "reason": reason, "fetched": 0, "network_called": False,
            "token_present": bool(settings.token), "permission_status": settings.collection_permission_status,
            "storage_permission_status": settings.storage_permission_status,
            "redistribution_status": settings.redistribution_status,
            "policy_reviewed_at": settings.policy_reviewed_at,
            "retention_mode": "full_text" if settings.storage_permission_status == "full_text" else "references_only",
            "authenticated_verification": "NOT_PERFORMED", "coverage_unknown": True}


@contextmanager
def collector_lock(root):
    path = Path(root) / "data/state/threads_collector.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CollectorError("BUSY", "Another Threads collector holds the lock") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def jsonl_rows(path):
    if not path.exists():
        return
    with path.open("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                break
            try:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError
            except (ValueError, UnicodeDecodeError):
                raise CollectorError("CORRUPT_STORAGE", "Invalid complete JSONL line; manual repair required") from None
            yield value


def append_once(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    # Recover an incomplete tail after a process/power interruption. Complete
    # malformed lines are an error, never silently dropped.
    if path.exists():
        with path.open("rb+") as stream:
            complete_end = 0
            for line in stream:
                if not line.endswith(b"\n"):
                    stream.truncate(complete_end)
                    stream.flush()
                    os.fsync(stream.fileno())
                    break
                complete_end = stream.tell()
        if any(old.get("event_id") == row["event_id"] for old in jsonl_rows(path)):
            return False
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(path.parent)
    return True


class Store:
    def __init__(self, root, settings):
        self.root, self.settings = Path(root), settings
        self.events_emitted = 0
        self.event_spools = set()
        self.noise_terms = []
        keyword_config = self.root / "config/threads_keywords.json"
        if keyword_config.exists():
            try:
                config = json.loads(keyword_config.read_text(encoding="utf-8"))
                noise = config.get("classification_only_noise_terms", []) if isinstance(config, dict) else []
                if isinstance(noise, list):
                    self.noise_terms = [term for term in noise if isinstance(term, str) and term.strip()]
            except ValueError:
                raise CollectorError("CONFIG_ERROR", "Malformed keyword configuration") from None
        path = self.root / "data/state/threads_collector.sqlite"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS posts(post_id TEXT PRIMARY KEY, content_hash TEXT, version INTEGER, last_observed_at TEXT);
        CREATE TABLE IF NOT EXISTS version_counters(post_id TEXT PRIMARY KEY,highest_version INTEGER);
        CREATE TABLE IF NOT EXISTS versions(platform TEXT, post_id TEXT, content_hash TEXT, version INTEGER,
            record_json TEXT, observed_at TEXT, PRIMARY KEY(platform,post_id,content_hash), UNIQUE(post_id,version));
        CREATE TABLE IF NOT EXISTS keyword_hits(post_id TEXT, query TEXT, first_seen TEXT, last_seen TEXT,
            PRIMARY KEY(post_id,query));
        CREATE TABLE IF NOT EXISTS checkpoints(query TEXT PRIMARY KEY, window_end TEXT);
        CREATE TABLE IF NOT EXISTS pending_windows(mode TEXT,query TEXT,window_start TEXT,window_end TEXT,
            cursor TEXT,seen_cursors TEXT,request_signature TEXT,PRIMARY KEY(mode,query));
        CREATE TABLE IF NOT EXISTS outbox(event_id TEXT PRIMARY KEY,post_id TEXT,version INTEGER,
            record_json TEXT,event_json TEXT,raw_path TEXT,event_path TEXT,raw_done INTEGER DEFAULT 0,event_done INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS suppressions(post_id TEXT PRIMARY KEY,suppressed_at TEXT);
        CREATE TABLE IF NOT EXISTS purged_versions(post_id TEXT,version INTEGER,PRIMARY KEY(post_id,version));
        CREATE TABLE IF NOT EXISTS budgets(day TEXT PRIMARY KEY,requests INTEGER,last_request_at REAL);
        CREATE TABLE IF NOT EXISTS control(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS exports(path TEXT PRIMARY KEY);
        """)
        with self.db:
            self.db.execute("INSERT INTO version_counters SELECT post_id,MAX(version) FROM versions GROUP BY post_id ON CONFLICT(post_id) DO UPDATE SET highest_version=MAX(highest_version,excluded.highest_version)")

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def checkpoint(self, query):
        row = self.db.execute("SELECT window_end FROM checkpoints WHERE query=?", (query,)).fetchone()
        return row[0] if row else None

    def set_checkpoint(self, query, end):
        old = self.checkpoint(query)
        # Historical backfill cannot rewind the daily high-water mark.
        if old and parse_time(old) >= parse_time(end):
            return
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?)", (query, end))

    def pending_window(self, mode, query):
        row = self.db.execute("SELECT * FROM pending_windows WHERE mode=? AND query=?", (mode, query)).fetchone()
        return dict(row) if row else None

    def save_window(self, mode, query, start, end, cursor, seen_cursors, request_signature):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO pending_windows VALUES (?,?,?,?,?,?,?)",
                            (mode, query, stamp(start), stamp(end), cursor,
                             json.dumps(sorted(seen_cursors)), request_signature))

    def complete_window(self, mode, query, end):
        # Commit the completed high-water mark and pending-window removal together.
        old = self.checkpoint(query)
        with self.db:
            if not old or parse_time(old) < parse_time(end):
                self.db.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?)", (query, end))
            self.db.execute("DELETE FROM pending_windows WHERE mode=? AND query=?", (mode, query))

    def reserve_request(self, day, sleep):
        cooldown = self.db.execute("SELECT value FROM control WHERE key='retry_not_before'").fetchone()
        now = time.time()
        if cooldown and float(cooldown[0]) > now:
            raise CollectorError("THROTTLED", "Persisted server cooldown is active", float(cooldown[0]) - now)
        row = self.db.execute("SELECT requests,last_request_at FROM budgets WHERE day=?", (day,)).fetchone()
        count, last = (row[0], row[1]) if row else (0, 0)
        if count >= self.settings.daily_max_requests:
            raise CollectorError("QUOTA_EXHAUSTED", "Local daily request budget exhausted")
        # Count every HTTP attempt, including retries, before the network call.
        if last:
            sleep(max(0, self.settings.request_interval - (now - last)))
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO budgets VALUES (?,?,?)", (day, count + 1, time.time()))

    def ingest(self, post, query, observed_at, historical_replay=False):
        pid = post.get("id")
        text = post.get("text")
        if not isinstance(pid, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", pid):
            return "rejected"
        if self.db.execute("SELECT 1 FROM suppressions WHERE post_id=?", (pid,)).fetchone():
            return "rejected"
        # Media-only posts are references, not fabricated text or claims.
        if text is not None and not isinstance(text, str):
            return "rejected"
        try:
            published = stamp(parse_time(post["timestamp"]))
            observed = stamp(parse_time(observed_at))
        except (KeyError, TypeError, ValueError, OverflowError):
            return "rejected"
        link = post.get("permalink")
        try:
            parsed = urlparse(link) if isinstance(link, str) else None
            if not parsed or parsed.scheme != "https" or parsed.hostname not in {"threads.net", "www.threads.net", "threads.com", "www.threads.com"} or parsed.username or parsed.password or parsed.port not in {None, 443}:
                return "rejected"
        except ValueError:
            return "rejected"
        # Strip tracking/query fragments from permalinks; never retain API URLs.
        permalink = parsed._replace(query="", fragment="").geturl()
        media_type = post.get("media_type", "")
        if not isinstance(media_type, str):
            return "rejected"
        content_hash = digest(json.dumps({"text": text or "", "media_type": media_type}, ensure_ascii=False, sort_keys=True))
        previous = self.db.execute("SELECT highest_version FROM version_counters WHERE post_id=?", (pid,)).fetchone()
        snapshot = self.db.execute("SELECT version FROM versions WHERE platform='threads' AND post_id=? AND content_hash=?", (pid, content_hash)).fetchone()
        with self.db:
            self.db.execute("INSERT INTO keyword_hits VALUES (?,?,?,?) ON CONFLICT(post_id,query) DO UPDATE SET last_seen=excluded.last_seen", (pid, query, observed, observed))
            if snapshot:
                self.db.execute("UPDATE posts SET last_observed_at=? WHERE post_id=?", (observed, pid))
                return "unchanged"
            version = previous[0] + 1 if previous else 1
            event_type = "threads.post.discovered" if previous is None else "threads.post.updated"
            eid = digest(f"threads|{pid}|{version}|{event_type}")
            rights = {"collection_permission_status": self.settings.collection_permission_status,
                      "storage_permission_status": self.settings.storage_permission_status,
                      "redistribution_status": self.settings.redistribution_status,
                      "policy_reviewed_at": self.settings.policy_reviewed_at,
                      "source_terms_url": self.settings.source_terms_url}
            classification = classify_post(text or "", search_queries=[query], noise_terms=self.noise_terms)
            record = {"post_id": pid, "permalink": permalink, "published_at": published,
                      "observed_at": observed, "source_id": "threads", "source_platform": "threads",
                      "source_role": "threads_post_candidate", "search_queries": [query],
                      "content_hash": content_hash, "version": version, "event_id": eid,
                      "event_type": event_type, "collection_method": "official_threads_api",
                      "license_status": "restricted_platform_content", "original_paid_ad_confirmed": False,
                      "review_status": "unreviewed", "historical_replay": bool(historical_replay),
                      "media_type": media_type, **classification, **rights}
            full_text = self.settings.storage_permission_status == "full_text" and isinstance(text, str)
            if full_text:
                record["raw_text"] = text
            record["retention_mode"] = "full_text" if full_text else "references_only"
            payload = {k: record[k] for k in ("post_id", "permalink", "published_at", "content_hash", "version", "source_role", "source_platform", "retention_mode")}
            event = {"event_id": eid, "event_type": event_type, "post_id": pid,
                     "source_id": "threads", "source_platform": "threads", "source_role": "threads_post_candidate",
                     "source_version": version, "event_time": published, "ingestion_time": observed,
                     "is_replay": bool(historical_replay), "historical_replay": bool(historical_replay),
                     "original_paid_ad_confirmed": False, "payload": payload}
            raw_path = "data/raw/threads_posts.jsonl" if full_text else "data/raw/threads_post_refs.jsonl"
            event_path = "data/events/threads_events_" + parse_time(observed).astimezone(VN).strftime("%Y%m%d") + ".jsonl"
            encoded = json.dumps(record, ensure_ascii=False)
            self.db.execute("INSERT INTO versions VALUES ('threads',?,?,?,?,?)", (pid, content_hash, version, encoded, observed))
            self.db.execute("INSERT OR REPLACE INTO posts VALUES (?,?,?,?)", (pid, content_hash, version, observed))
            self.db.execute("INSERT OR REPLACE INTO version_counters VALUES (?,?)", (pid, version))
            self.db.execute("INSERT INTO outbox(event_id,post_id,version,record_json,event_json,raw_path,event_path) VALUES (?,?,?,?,?,?,?)", (eid, pid, version, encoded, json.dumps(event, ensure_ascii=False), raw_path, event_path))
        return "new" if previous is None else "updated"

    def flush_outbox(self, crash_hook=None):
        # Resume explicitly requested deletions before sending pending events.
        # Durable tombstones protect the cleanup-to-SQL crash window too.
        for (post_id,) in self.db.execute("SELECT post_id FROM suppressions WHERE post_id IN (SELECT post_id FROM versions)").fetchall():
            self.suppress(post_id)
        pending_purges = {(row[0], row[1]) for row in self.db.execute("SELECT p.post_id,p.version FROM purged_versions p JOIN versions v ON v.post_id=p.post_id AND v.version=p.version")}
        if pending_purges:
            self._finish_purge(pending_purges)
        for row in self.db.execute("SELECT o.* FROM outbox o WHERE (raw_done=0 OR event_done=0) AND NOT EXISTS(SELECT 1 FROM suppressions s WHERE s.post_id=o.post_id) ORDER BY o.rowid").fetchall():
            if not row["raw_done"]:
                append_once(self.root / row["raw_path"], json.loads(row["record_json"]))
                if crash_hook:
                    crash_hook("after_raw_append")
                with self.db:
                    self.db.execute("UPDATE outbox SET raw_done=1 WHERE event_id=?", (row["event_id"],))
            if not row["event_done"]:
                if append_once(self.root / row["event_path"], json.loads(row["event_json"])):
                    self.events_emitted += 1
                    self.event_spools.add(row["event_path"])
                if crash_hook:
                    crash_hook("after_event_append")
                with self.db:
                    self.db.execute("UPDATE outbox SET event_done=1 WHERE event_id=?", (row["event_id"],))

    def export(self, path, include_text=False, now=None):
        cutoff = stamp((now or datetime.now(UTC)) - timedelta(days=self.settings.retention_days))
        records = []
        for row in self.db.execute("SELECT v.record_json FROM versions v JOIN posts p ON v.post_id=p.post_id AND v.version=p.version WHERE v.observed_at>=? AND NOT EXISTS(SELECT 1 FROM suppressions s WHERE s.post_id=v.post_id) AND NOT EXISTS(SELECT 1 FROM purged_versions d WHERE d.post_id=v.post_id AND d.version=v.version)", (cutoff,)):
            record = json.loads(row[0])
            if include_text and record.get("raw_text") and self.settings.redistribution_status != "permitted":
                raise CollectorError("BLOCKED_POLICY", "Text export requires explicitly permitted redistribution")
            record["search_queries"] = [x[0] for x in self.db.execute("SELECT query FROM keyword_hits WHERE post_id=? ORDER BY query", (record["post_id"],))]
            if not include_text:
                record.pop("raw_text", None)
            records.append(record)
        write_jsonl(Path(path), records)
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO exports VALUES (?)", (str(Path(path).resolve()),))
        return len(records)

    def _remove_payloads(self, keys, ids=()):
        ids, keys = set(ids), set(keys)
        paths = list((self.root / "data/raw").glob("threads*.jsonl"))
        paths += list((self.root / "data/events").glob("*.jsonl"))
        paths += [Path(row[0]) for row in self.db.execute("SELECT path FROM exports")]
        for path in set(paths):
            if not path.exists():
                continue
            rows = list(jsonl_rows(path))
            kept = []
            for row in rows:
                pid = row.get("post_id") or (row.get("payload") or {}).get("post_id")
                version = row.get("version", row.get("source_version"))
                threads_row = path.parent.name != "events" or row.get("source_platform") == "threads" or str(row.get("event_type", "")).startswith("threads.post.")
                if not threads_row or (pid not in ids and (pid, version) not in keys):
                    kept.append(row)
            if len(kept) != len(rows):
                write_jsonl(path, kept)
        # Known generated review files are derived copies, never user annotation.
        import csv
        for path in (self.root / "data/auto_baseline/threads").glob("*.csv"):
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                columns = reader.fieldnames
                rows = list(reader)
            def permitted_review_row(row):
                pid = row.get("post_id")
                if pid in ids:
                    return False
                try:
                    version = int(row.get("source_version") or 0)
                except ValueError:
                    version = 0
                return (pid, version) not in keys and (version != 0 or pid not in {key[0] for key in keys})
            kept = [row for row in rows if permitted_review_row(row)]
            if len(kept) != len(rows) and columns:
                fd, temporary = tempfile.mkstemp(dir=path.parent)
                with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(kept)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)

    def suppress(self, post_id):
        # Tombstone first, so interruption cannot reintroduce revoked content.
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO suppressions VALUES (?,?)", (post_id, stamp()))
        self._remove_payloads(set(), {post_id})
        with self.db:
            for table in ("posts", "versions", "outbox", "keyword_hits"):
                self.db.execute(f"DELETE FROM {table} WHERE post_id=?", (post_id,))
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def purge_expired(self, now=None):
        cutoff = stamp((now or datetime.now(UTC)) - timedelta(days=self.settings.retention_days))
        expired = self.db.execute("SELECT post_id,version FROM versions WHERE observed_at<?", (cutoff,)).fetchall()
        keys = {(row[0], row[1]) for row in expired}
        with self.db:
            self.db.executemany("INSERT OR IGNORE INTO purged_versions VALUES (?,?)", keys)
        self._finish_purge(keys)
        return len(keys)

    def _finish_purge(self, keys):
        self._remove_payloads(keys)
        with self.db:
            for pid, version in keys:
                self.db.execute("DELETE FROM versions WHERE post_id=? AND version=?", (pid, version))
                self.db.execute("DELETE FROM outbox WHERE post_id=? AND version=?", (pid, version))
                self.db.execute("DELETE FROM posts WHERE post_id=? AND version=?", (pid, version))
            self.db.execute("DELETE FROM keyword_hits WHERE post_id NOT IN (SELECT post_id FROM posts)")
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def is_suppressed(root, post_id):
    path = Path(root) / "data/state/threads_collector.sqlite"
    if not path.exists():
        return False
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return bool(db.execute("SELECT 1 FROM suppressions WHERE post_id=?", (post_id,)).fetchone())


def load_keywords(root):
    try:
        obj = json.loads((Path(root) / "config/threads_keywords.json").read_text(encoding="utf-8"))
        values = obj["keywords"] if isinstance(obj, dict) else obj
        if not isinstance(values, list) or not values or any(not isinstance(x, str) or not x.strip() or len(x) > 200 for x in values):
            raise ValueError
        return list(dict.fromkeys(x.strip() for x in values))
    except (OSError, ValueError, KeyError):
        raise CollectorError("CONFIG_ERROR", "Invalid or missing keyword configuration") from None


def dataset_report(root=ROOT):
    root = Path(root)
    result = {"threads_unique_posts": 0, "threads_content_versions": 0, "promotion_candidates": 0,
              "manual_gold": 0, "manual_gold_status": "not_registered", "suppressed": 0,
              "raw_threads_full_text_rows": 0, "raw_threads_reference_rows": 0,
              "legacy_promotion_rows": 0, "evidence_candidate_rows": 0,
              "research_metadata_rows": 0, "product_metadata_rows": 0, "external_viecomner_physical_rows": 0}
    path = root / "data/state/threads_collector.sqlite"
    if path.exists():
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            result["threads_unique_posts"] = db.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            result["threads_content_versions"] = db.execute("SELECT COUNT(*) FROM versions").fetchone()[0]
            result["suppressed"] = db.execute("SELECT COUNT(*) FROM suppressions").fetchone()[0]
            for (encoded,) in db.execute("SELECT v.record_json FROM versions v JOIN posts p ON p.post_id=v.post_id AND p.version=v.version"):
                if json.loads(encoded).get("post_role") in {"brand_promotional_post", "affiliate_promotion_candidate"}:
                    result["promotion_candidates"] += 1
    names = {"threads_posts.jsonl": "raw_threads_full_text_rows", "threads_post_refs.jsonl": "raw_threads_reference_rows",
             "posts_auto.jsonl": "legacy_promotion_rows", "evidence_discovery.jsonl": "evidence_candidate_rows",
             "research_metadata.jsonl": "research_metadata_rows", "product_metadata.jsonl": "product_metadata_rows"}
    for name, key in names.items():
        file = root / "data/raw" / name
        if file.exists():
            with file.open("rb") as stream:
                result[key] = sum(1 for line in stream if line.strip() and line.endswith(b"\n"))
    for file in (root / "data/external/ViEcomNER").glob("*.jsonl"):
        with file.open("rb") as stream:
            result["external_viecomner_physical_rows"] += sum(1 for line in stream if line.strip())
    return result


def run_collection(root=ROOT, mode="daily", enable_live=False, since=None, until=None,
                   max_pages_per_keyword=None, settings=None, client=None, now=None, crash_hook=None):
    root = Path(root)
    now = parse_time(now or datetime.now(UTC)).replace(microsecond=0)
    report = {"started_at": stamp(now), "mode": mode, "status": "STARTED", "keyword_count": 0,
              "queries_attempted": 0, "pages": 0, "requests": 0, "fetched": 0, "unique_ids": 0,
              "new": 0, "updated": 0, "unchanged": 0, "rejected": 0, "blocked": 0,
              "throttled": 0, "errors": [], "checkpoint_ranges": [], "coverage_unknown": True,
              "live_api_status": "NOT_CALLED", "historical_replay": mode == "backfill",
              "events_emitted": 0, "event_spools": []}
    def finish():
        report["finished_at"] = stamp()
        report["dataset"] = dataset_report(root)
        atomic_json(root / "data/reports/threads_daily_latest.json", report)
        return report
    try:
        settings = settings or Settings.from_env(root)
    except ValueError:
        report.update(status="CONFIG_ERROR", errors=["Invalid configuration; see template"])
        return finish()
    permission = audit(root, settings, now)
    report.update({k: permission[k] for k in ("permission_status", "storage_permission_status", "redistribution_status", "retention_mode") if k in permission})
    if not (settings.enable_daily or enable_live):
        report["status"] = "SKIPPED_CONFIG_DISABLED"
        return finish()
    if permission["status"] != "READY":
        report.update(status=permission["status"], blocked=1, errors=[permission["reason"]], live_api_status=permission["status"])
        return finish()
    ids = set()
    store = None
    try:
        if mode not in {"daily", "backfill", "verify"}:
            raise CollectorError("CONFIG_ERROR", "Unsupported collection mode")
        keywords = load_keywords(root)
        report["keyword_count"] = len(keywords)
        cap = max_pages_per_keyword if max_pages_per_keyword is not None else settings.max_pages_per_keyword
        if not isinstance(cap, int) or not 1 <= cap <= 100:
            raise CollectorError("CONFIG_ERROR", "Page cap must be 1..100")
        backfill_start = backfill_end = None
        if mode == "backfill":
            try:
                backfill_start, backfill_end = parse_time(since), parse_time(until)
                if not API_MIN_TIME <= backfill_start.timestamp() < backfill_end.timestamp() <= now.timestamp() or backfill_end - backfill_start > timedelta(days=settings.max_backfill_days):
                    raise ValueError
            except (TypeError, ValueError):
                raise CollectorError("CONFIG_ERROR", "Backfill needs past since<until within configured maximum days") from None
        with collector_lock(root), Store(root, settings) as store:
            store.flush_outbox(crash_hook)
            actual_client = client or ThreadsClient(settings)
            day = now.astimezone(VN).date().isoformat()
            def reserve():
                actual_day = datetime.now(UTC).astimezone(VN).date().isoformat() if client is None else day
                store.reserve_request(actual_day, getattr(actual_client, "sleep", lambda seconds: None))
                report["requests"] += 1
            if isinstance(actual_client, ThreadsClient):
                actual_client.before_request = reserve
            partial = False
            pending_mode = ("backfill:" + stamp(backfill_start) + ":" + stamp(backfill_end)
                            if mode == "backfill" else mode)
            signature = digest(json.dumps({"search_type": settings.search_type, "search_mode": settings.search_mode,
                                           "fields": settings.fields, "limit": settings.page_limit}, sort_keys=True))
            queries = keywords[:1] if mode == "verify" else keywords
            if mode != "verify":
                # Finish interrupted queries before starting fresh windows, so a
                # small daily budget cannot permanently starve later keywords.
                queries = sorted(queries, key=lambda query: store.pending_window(pending_mode, query) is None)
            for query in queries:
                checkpoint = store.checkpoint(query)
                pending = store.pending_window(pending_mode, query) if mode != "verify" else None
                cursor, cursors, completed = None, set(), False
                clamped = False
                if pending:
                    if pending["request_signature"] != signature:
                        raise CollectorError("CONFIG_ERROR", "Pending pagination requires the original search type, mode, fields and page size")
                    try:
                        start, end = parse_time(pending["window_start"]), parse_time(pending["window_end"])
                        cursor = pending["cursor"]
                        cursors = set(json.loads(pending["seen_cursors"]))
                        if any(not isinstance(value, str) for value in cursors):
                            raise ValueError
                    except (TypeError, ValueError):
                        raise CollectorError("STORAGE_ERROR", "Invalid pending pagination state") from None
                else:
                    end = backfill_end or now
                    start = backfill_start or ((parse_time(checkpoint) if checkpoint else now) - timedelta(hours=settings.lookback_hours))
                    if start < end - timedelta(days=settings.max_backfill_days):
                        start, clamped = end - timedelta(days=settings.max_backfill_days), True
                    start = max(start, datetime.fromtimestamp(API_MIN_TIME, UTC))
                if start >= end or end > now:
                    raise CollectorError("CONFIG_ERROR", "Invalid checkpoint time window")
                window = {"query": query, "since": stamp(start), "until": stamp(end), "checkpoint_before": checkpoint,
                          "completed": False, "clamped_to_max_days": clamped, "resumed_window": pending is not None}
                report["checkpoint_ranges"].append(window)
                report["queries_attempted"] += 1
                if mode != "verify" and pending is None:
                    # Persist the fixed range before the first request. A quota,
                    # timeout or crash must not move the next run's lower bound.
                    store.save_window(pending_mode, query, start, end, cursor, cursors, signature)
                for page_number in range(1 if mode == "verify" else cap):
                    params = {"q": query, "search_type": settings.search_type, "search_mode": settings.search_mode,
                              "fields": settings.fields, "limit": 1 if mode == "verify" else settings.page_limit,
                              "since": int(start.timestamp()), "until": int(end.timestamp())}
                    if cursor:
                        params["after"] = cursor
                    if not isinstance(actual_client, ThreadsClient):
                        reserve()
                    obj = validate_response(actual_client.search(params))
                    report["live_api_status"] = "VERIFIED_WITH_TOKEN" if client is None else "INJECTED_OFFLINE_CLIENT"
                    report["pages"] += 1
                    report["fetched"] += len(obj["data"])
                    for post in obj["data"]:
                        outcome = store.ingest(post, query, stamp(now), historical_replay=mode == "backfill")
                        report[outcome] += 1
                        if outcome != "rejected":
                            ids.add(post["id"])
                        if outcome in {"new", "updated"} and crash_hook:
                            crash_hook("after_state")
                    store.flush_outbox(crash_hook)
                    paging = obj.get("paging", {})
                    after = paging.get("cursors", {}).get("after")
                    # No server next link => end, even if cursors remain in response.
                    if not obj["data"] or not paging.get("next"):
                        completed = True
                        break
                    if not isinstance(after, str) or not after or len(after) > 4096 or after in cursors:
                        window["stop_reason"] = "missing_or_repeated_cursor"
                        break
                    cursors.add(after)
                    cursor = after
                    if mode != "verify":
                        # Advance only after every record/event in this page is
                        # durable. Crash before this commit replays the page using
                        # the existing idempotent post/version/event identities.
                        store.save_window(pending_mode, query, start, end, cursor, cursors, signature)
                window["completed"] = completed
                if completed and mode != "verify":
                    store.complete_window(pending_mode, query, stamp(end))
                else:
                    partial = True
                    window.setdefault("stop_reason", "page_cap")
                window["checkpoint_after"] = store.checkpoint(query)
            report["status"] = "PARTIAL" if partial else "COMPLETED"
    except CollectorError as exc:
        report["status"] = exc.status
        report["errors"].append(str(exc))
        report["blocked"] = int(exc.status.startswith("BLOCKED"))
        report["throttled"] = int(exc.status in {"THROTTLED", "QUOTA_EXHAUSTED"})
        if exc.retry_after is not None:
            report["retry_after_seconds"] = exc.retry_after
            # Persist across invocations to honor 429 even when Cron reruns.
            with collector_lock(root), Store(root, settings) as cooldown_store:
                with cooldown_store.db:
                    cooldown_store.db.execute("INSERT OR REPLACE INTO control VALUES ('retry_not_before',?)", (str(time.time() + exc.retry_after),))
        if exc.status in {"BLOCKED_PERMISSION", "NETWORK_UNAVAILABLE"}:
            report["live_api_status"] = exc.status
    except (OSError, sqlite3.Error):
        report.update(status="STORAGE_ERROR", errors=["Storage failure; details withheld"])
    if store is not None:
        # Count actual durable writes, including a recovered pending outbox.
        # Ingested versions, suppressed/purged rows, and already appended retries
        # must not inflate the number of events emitted by this invocation.
        report["events_emitted"] = store.events_emitted
        report["event_spools"] = sorted(store.event_spools)
    report["unique_ids"] = len(ids)
    return finish()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    audit_parser = commands.add_parser("audit", help="local configuration/permission audit, no network by default")
    audit_parser.add_argument("--verify-live", action="store_true", help="explicitly run one permitted API test query")
    commands.add_parser("self-test", help="run synthetic offline tests in temporary directories")
    for name in ("daily", "backfill"):
        command = commands.add_parser(name)
        command.add_argument("--max-pages-per-keyword", type=int)
        if name == "backfill":
            command.add_argument("--since", required=True)
            command.add_argument("--until", required=True)
            command.add_argument("--enable-live", action="store_true")
    commands.add_parser("report")
    suppress_parser = commands.add_parser("suppress", help="explicitly suppress and remove a Threads post from managed copies")
    suppress_parser.add_argument("--post-id", required=True)
    commands.add_parser("purge", help="explicitly enforce configured retention on managed Threads data")
    export_parser = commands.add_parser("export", help="export latest unsuppressed references")
    export_parser.add_argument("--output", required=True, type=Path)
    export_parser.add_argument("--include-text", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "self-test":
        print("SYNTHETIC OFFLINE TEST ONLY: temporary test roots; no live collection", flush=True)
        return subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_threads*.py", "-v"], cwd=ROOT, check=False).returncode
    if args.command == "audit":
        result = run_collection(mode="verify", enable_live=True, max_pages_per_keyword=1) if args.verify_live else audit()
    elif args.command == "report":
        latest = ROOT / "data/reports/threads_daily_latest.json"
        result = {"dataset": dataset_report(), "last_run": json.loads(latest.read_text()) if latest.exists() else None}
    elif args.command in {"daily", "backfill"}:
        result = run_collection(mode=args.command, enable_live=getattr(args, "enable_live", False),
                                since=getattr(args, "since", None), until=getattr(args, "until", None),
                                max_pages_per_keyword=args.max_pages_per_keyword)
    else:
        settings = Settings.from_env(ROOT)
        with collector_lock(ROOT), Store(ROOT, settings) as store:
            if args.command == "suppress":
                store.suppress(args.post_id)
                result = {"status": "SUPPRESSED"}
            elif args.command == "purge":
                result = {"status": "PURGED", "versions_removed": store.purge_expired()}
            else:
                if args.include_text and settings.redistribution_status != "permitted":
                    raise CollectorError("BLOCKED_POLICY", "Text export requires explicitly permitted redistribution")
                result = {"status": "EXPORTED", "rows": store.export(args.output, args.include_text)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    status = result.get("status", "COMPLETED")
    return 0 if status in {"READY", "COMPLETED", "PARTIAL", "SKIPPED_CONFIG_DISABLED", "SUPPRESSED", "PURGED", "EXPORTED"} else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (CollectorError, ValueError, OSError, sqlite3.Error):
        print(json.dumps({"status": "FAILED", "reason": "Operation failed; no sensitive exception details logged"}))
        sys.exit(2)
