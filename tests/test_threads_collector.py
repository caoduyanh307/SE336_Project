"""SYNTHETIC OFFLINE TEST ONLY: no fixture touches production data or network."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from ingestion import threads_collector as m


NOW = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)
SECRET = "SYNTHETIC_TEST_TOKEN_DO_NOT_LOG"
TEXT = "SYNTHETIC OFFLINE TEST ONLY. Collagen hỗ trợ sức khỏe. Mua ngay."


def fixture_post(post_id="synthetic-1", text=TEXT, **changes):
    result = {
        "id": post_id,
        "text": text,
        "timestamp": "2026-11-02T12:00:00+0000",
        "permalink": "https://www.threads.net/@synthetic/post/TEST",
        "media_type": "TEXT_POST",
    }
    result.update(changes)
    return result


def page(items=(), after=None):
    result = {"data": list(items)}
    if after is not None:
        result["paging"] = {
            "cursors": {"after": after},
            "next": "https://graph.threads.net/keyword_search?after=" + after,
        }
    return result


class FakeClient:
    """Prepared API responses; deliberately has no actual HTTP transport."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def search(self, params):
        self.calls.append(dict(params))
        if not self.responses:
            raise AssertionError("unexpected additional synthetic API request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class Response:
    def __init__(self, value):
        self.payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.headers = {}
        self.status = 200

    def read(self, *args):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def lines(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="threads-synthetic-offline-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "config").mkdir()
        self.keywords("collagen", "vitamin C")
        self.env = {
            "SE363_ENABLE_THREADS_DAILY": "1",
            "THREADS_ACCESS_TOKEN": SECRET,
            "THREADS_COLLECTION_PERMISSION_STATUS": "verified",
            "THREADS_VERIFIED_SCOPES": "threads_basic,threads_keyword_search",
            "THREADS_STORAGE_PERMISSION_STATUS": "full_text",
            "THREADS_REDISTRIBUTION_STATUS": "not_permitted",
            "THREADS_POLICY_REVIEWED_AT": "2026-11-03",
            "THREADS_SOURCE_TERMS_URL": "https://developers.facebook.com/terms/",
            "THREADS_RETENTION_DAYS": "30",
            "THREADS_MAX_PAGES_PER_KEYWORD": "2",
            "THREADS_DAILY_MAX_REQUESTS": "30",
            "THREADS_REQUEST_INTERVAL_SECONDS": "4",
        }

    def keywords(self, *queries):
        (self.root / "config" / "threads_keywords.json").write_text(
            json.dumps({"keywords": list(queries), "noise_terms": []}, ensure_ascii=False),
            encoding="utf-8",
        )

    def settings(self, **overrides):
        return m.Settings.from_env(self.root, environ={**self.env, **overrides})

    def run_job(self, client, **kwargs):
        return m.run_collection(
            root=self.root, settings=kwargs.pop("settings", self.settings()),
            client=client, now=kwargs.pop("now", NOW), **kwargs,
        )

    def store(self, settings=None):
        store = m.Store(self.root, settings or self.settings())
        self.addCleanup(store.db.close)
        return store

    def raw(self, refs=False):
        return lines(self.root / "data" / "raw" / (
            "threads_post_refs.jsonl" if refs else "threads_posts.jsonl"))

    def events(self):
        return [row for file in (self.root / "data" / "events").glob("*.jsonl")
                for row in lines(file)]


class PermissionTests(CollectorCase):
    def test_audit_missing_token_does_not_call_network(self):
        with patch.object(m, "urlopen", side_effect=AssertionError("network forbidden")):
            report = m.audit(root=self.root, settings=self.settings(THREADS_ACCESS_TOKEN=""))
        self.assertEqual(report["status"], "BLOCKED_PERMISSION")
        self.assertEqual(report.get("fetched", 0), 0)
        self.assertNotIn(SECRET, json.dumps(report))

    def test_audit_missing_scope_does_not_call_network(self):
        with patch.object(m, "urlopen", side_effect=AssertionError("network forbidden")):
            report = m.audit(root=self.root, settings=self.settings(THREADS_VERIFIED_SCOPES="threads_basic"))
        self.assertEqual(report["status"], "BLOCKED_PERMISSION")

    def test_disabled_daily_has_zero_live_requests(self):
        client = FakeClient([])
        report = self.run_job(client, settings=self.settings(SE363_ENABLE_THREADS_DAILY="0"))
        self.assertEqual(report["status"], "SKIPPED_CONFIG_DISABLED")
        self.assertEqual(report["fetched"], 0)
        self.assertEqual(client.calls, [])

    def test_missing_token_blocks_enabled_daily(self):
        client = FakeClient([])
        report = self.run_job(client, settings=self.settings(THREADS_ACCESS_TOKEN=""))
        self.assertEqual(report["status"], "BLOCKED_PERMISSION")
        self.assertEqual(report["fetched"], 0)
        self.assertEqual(client.calls, [])

    def test_unreviewed_storage_policy_blocks_live(self):
        client = FakeClient([])
        report = self.run_job(client, settings=self.settings(THREADS_STORAGE_PERMISSION_STATUS="unverified"))
        self.assertEqual(report["status"], "BLOCKED_POLICY")
        self.assertEqual(client.calls, [])

    def test_unapproved_endpoint_never_receives_bearer(self):
        with patch.object(m, "urlopen", side_effect=AssertionError("network forbidden")):
            try:
                settings = self.settings(THREADS_API_BASE="https://example.org/steal")
                report = m.audit(root=self.root, settings=settings)
            except ValueError:
                return
        self.assertNotIn(report["status"], {"READY", "VERIFIED_WITH_TOKEN", "COMPLETED"})


class IncrementalTests(CollectorCase):
    def test_pagination_cursor_and_cross_keyword_deduplication(self):
        one = fixture_post()
        two = fixture_post("synthetic-2", TEXT + " Vitamin C.")
        client = FakeClient([page([one], "cursor1"), page([one, two]), page([one])])
        report = self.run_job(client)
        self.assertEqual([c.get("after") for c in client.calls], [None, "cursor1", None])
        self.assertEqual(report["new"], 2)
        self.assertEqual(report["updated"], 0)
        self.assertEqual(report["unique_ids"], 2)
        self.assertEqual(len(self.raw()), 2)
        self.assertEqual(len(self.events()), 2)
        store = self.store()
        self.assertIsNotNone(store.checkpoint("collagen"))
        self.assertIsNotNone(store.checkpoint("vitamin C"))
        export = self.root / "export.jsonl"
        store.export(export, now=NOW)
        first = next(row for row in lines(export) if row["post_id"] == "synthetic-1")
        self.assertEqual(set(first["search_queries"]), {"collagen", "vitamin C"})

    def test_identical_next_day_run_does_not_create_versions_or_events(self):
        one = fixture_post()
        self.run_job(FakeClient([page([one]), page([one])]))
        report = self.run_job(FakeClient([page([one]), page([one])]), now=NOW + timedelta(days=1))
        self.assertEqual(report["new"], 0)
        self.assertEqual(report["updated"], 0)
        self.assertGreater(report["unchanged"], 0)
        self.assertEqual(len(self.raw()), 1)
        self.assertEqual(len(self.events()), 1)

    def test_edited_text_versions_but_engagement_does_not(self):
        self.keywords("collagen")
        self.run_job(FakeClient([page([fixture_post(like_count=1)])]))
        unchanged = self.run_job(FakeClient([page([fixture_post(like_count=99, unknown_field="ignored")])]))
        self.assertEqual(unchanged["updated"], 0)
        changed = self.run_job(FakeClient([page([fixture_post(text=TEXT + " Công thức cập nhật.")])]))
        self.assertEqual(changed["updated"], 1)
        self.assertEqual(changed["new"], 0)
        rows = self.raw()
        self.assertEqual([r["version"] for r in rows], [1, 2])
        self.assertNotEqual(rows[0]["content_hash"], rows[1]["content_hash"])
        events = self.events()
        self.assertEqual([e["event_type"] for e in events], ["threads.post.discovered", "threads.post.updated"])
        self.assertEqual(events[-1]["source_version"], 2)
        self.assertNotIn(TEXT, json.dumps(events, ensure_ascii=False))

    def test_page_cap_does_not_advance_incomplete_keyword_checkpoint(self):
        self.keywords("collagen")
        client = FakeClient([page([fixture_post()], "more")])
        self.run_job(client, max_pages_per_keyword=1)
        self.assertEqual(len(client.calls), 1)
        self.assertIsNone(self.store().checkpoint("collagen"))

    def test_page_cap_resumes_same_window_and_cursor_next_day(self):
        self.keywords("collagen")
        first_client = FakeClient([page([fixture_post()], "page-2")])
        first = self.run_job(first_client, max_pages_per_keyword=1)
        self.assertEqual(first["status"], "PARTIAL")
        self.assertIsNone(self.store().checkpoint("collagen"))

        second_client = FakeClient([page([fixture_post("synthetic-2")])])
        second = self.run_job(second_client, max_pages_per_keyword=1, now=NOW + timedelta(days=1))
        self.assertEqual(second["status"], "COMPLETED")
        self.assertEqual(second_client.calls[0]["after"], "page-2")
        self.assertEqual(second_client.calls[0]["since"], first_client.calls[0]["since"])
        self.assertEqual(second_client.calls[0]["until"], first_client.calls[0]["until"])
        self.assertTrue(second["checkpoint_ranges"][0]["resumed_window"])
        self.assertEqual(self.store().checkpoint("collagen"), m.stamp(NOW))
        self.assertIsNone(self.store().pending_window("daily", "collagen"))
        self.assertEqual(len(self.raw()), 2)
        self.assertEqual(len({row["event_id"] for row in self.events()}), 2)

    def test_quota_interruption_preserves_cursor_after_durable_page(self):
        self.keywords("collagen")
        settings = self.settings(THREADS_DAILY_MAX_REQUESTS="1")
        first_client = FakeClient([page([fixture_post()], "page-2")])
        first = self.run_job(first_client, settings=settings, max_pages_per_keyword=2)
        self.assertEqual(first["status"], "QUOTA_EXHAUSTED")
        self.assertEqual(self.store(settings).pending_window("daily", "collagen")["cursor"], "page-2")
        second_client = FakeClient([page([fixture_post(), fixture_post("synthetic-2")])])
        second = self.run_job(second_client, settings=settings, now=NOW + timedelta(days=1))
        self.assertEqual(second["status"], "COMPLETED")
        self.assertEqual(second_client.calls[0]["after"], "page-2")
        self.assertEqual(second_client.calls[0]["until"], first_client.calls[0]["until"])
        self.assertEqual(second["new"], 1)
        self.assertEqual(second["unchanged"], 1)
        self.assertEqual(len(self.events()), 2)

    def test_pending_keyword_precedes_fresh_windows_under_small_budget(self):
        settings = self.settings(THREADS_DAILY_MAX_REQUESTS="1")
        first = self.run_job(FakeClient([page()]), settings=settings)
        self.assertEqual(first["status"], "QUOTA_EXHAUSTED")
        self.assertIsNotNone(self.store(settings).pending_window("daily", "vitamin C"))
        next_client = FakeClient([page()])
        self.run_job(next_client, settings=settings, now=NOW + timedelta(days=1))
        self.assertEqual(next_client.calls[0]["q"], "vitamin C")
        self.assertEqual(self.store(settings).checkpoint("vitamin C"), m.stamp(NOW))

    def test_crash_before_cursor_commit_replays_fixed_page_without_duplicate_events(self):
        self.keywords("collagen")
        first_client = FakeClient([page([fixture_post()], "page-2")])
        def crash(stage):
            if stage == "after_raw_append":
                raise RuntimeError("SYNTHETIC OFFLINE CRASH")
        with self.assertRaises(RuntimeError):
            self.run_job(first_client, crash_hook=crash)
        resumed = FakeClient([page([fixture_post()], "page-2"), page([fixture_post("synthetic-2")])])
        report = self.run_job(resumed, now=NOW + timedelta(days=1))
        self.assertEqual(report["status"], "COMPLETED")
        self.assertEqual(resumed.calls[0]["since"], first_client.calls[0]["since"])
        self.assertEqual(resumed.calls[0]["until"], first_client.calls[0]["until"])
        self.assertEqual([call.get("after") for call in resumed.calls], [None, "page-2"])
        self.assertEqual(len(self.raw()), 2)
        self.assertEqual(len(self.events()), 2)
        self.assertEqual(len({row["event_id"] for row in self.events()}), 2)

    def test_probe_and_separate_backfill_leave_daily_continuation_intact(self):
        self.keywords("collagen")
        first_client = FakeClient([page([fixture_post()], "daily-page-2")])
        self.run_job(first_client, max_pages_per_keyword=1)
        before = self.store().pending_window("daily", "collagen")
        self.run_job(FakeClient([page()]), mode="verify", now=NOW + timedelta(hours=1))
        self.assertEqual(self.store().pending_window("daily", "collagen"), before)
        self.run_job(FakeClient([page()]), mode="backfill", enable_live=True,
                     since="2026-10-30", until="2026-10-31")
        self.assertEqual(self.store().pending_window("daily", "collagen"), before)
        resumed = FakeClient([page([fixture_post("synthetic-2")])])
        self.run_job(resumed, now=NOW + timedelta(days=1))
        self.assertEqual(resumed.calls[0]["after"], "daily-page-2")
        self.assertEqual(resumed.calls[0]["until"], first_client.calls[0]["until"])
        self.assertEqual(self.store().checkpoint("collagen"), m.stamp(NOW))

    def test_repeated_cursor_stops_without_checkpoint(self):
        self.keywords("collagen")
        client = FakeClient([page([fixture_post()], "same"), page([fixture_post()], "same")])
        self.run_job(client, max_pages_per_keyword=5)
        self.assertLessEqual(len(client.calls), 2)
        self.assertIsNone(self.store().checkpoint("collagen"))

    def test_empty_page_ends_pagination(self):
        self.keywords("collagen")
        client = FakeClient([page([], "unused")])
        self.run_job(client)
        self.assertEqual(len(client.calls), 1)
        self.assertIsNotNone(self.store().checkpoint("collagen"))

    def test_window_uses_prior_checkpoint_minus_lookback(self):
        self.keywords("collagen")
        self.run_job(FakeClient([page()]))
        client = FakeClient([page()])
        self.run_job(client, now=NOW + timedelta(days=1))
        lower = client.calls[0]["since"]
        if isinstance(lower, str) and not lower.isdigit():
            lower = datetime.fromisoformat(lower.replace("Z", "+00:00")).timestamp()
        self.assertEqual(int(lower), int((NOW - timedelta(hours=48)).timestamp()))

    def test_backfill_requires_flag_and_labels_events_historical(self):
        self.keywords("collagen")
        denied = FakeClient([])
        settings = self.settings(SE363_ENABLE_THREADS_DAILY="0")
        report = self.run_job(denied, mode="backfill", settings=settings,
                              since="2026-11-01", until="2026-11-03")
        self.assertEqual(denied.calls, [])
        self.assertNotEqual(report["status"], "COMPLETED")
        allowed = FakeClient([page([fixture_post()])])
        self.run_job(allowed, mode="backfill", settings=settings, enable_live=True,
                     since="2026-11-01", until="2026-11-03")
        event = self.events()[0]
        self.assertTrue(event["historical_replay"])
        self.assertTrue(event["is_replay"])

    def test_future_or_excessive_backfill_does_not_call_api(self):
        self.keywords("collagen")
        for since, until in [("2026-11-03", "2026-11-05"), ("2025-01-01", "2026-11-03")]:
            with self.subTest(since=since, until=until):
                client = FakeClient([])
                try:
                    report = self.run_job(client, mode="backfill", enable_live=True,
                                          since=since, until=until)
                except ValueError:
                    pass
                else:
                    self.assertNotEqual(report["status"], "COMPLETED")
                self.assertEqual(client.calls, [])

    def test_malformed_permalinks_are_rejected_without_aborting_job(self):
        self.keywords("collagen")
        for permalink in ("https://[invalid", "https://evil.example/post", "javascript:alert(1)", "https://user:secret@www.threads.net/post/TEST"):
            with self.subTest(permalink=permalink):
                report = self.run_job(FakeClient([page([fixture_post(permalink=permalink)])]))
                self.assertEqual(report["new"], 0)
                self.assertEqual(report["rejected"], 1)
        self.assertEqual(self.raw(), [])

    def test_missing_id_response_is_rejected_not_fabricated(self):
        self.keywords("collagen")
        malformed = fixture_post()
        del malformed["id"]
        report = self.run_job(FakeClient([page([malformed])]))
        self.assertEqual(report["new"], 0)
        self.assertEqual(report["rejected"], 1)
        self.assertEqual(self.raw(), [])

    def test_daily_request_cap_persists_across_runs(self):
        settings = self.settings(THREADS_DAILY_MAX_REQUESTS="1")
        first = FakeClient([page([fixture_post()])])
        self.run_job(first, settings=settings)
        self.assertEqual(len(first.calls), 1)
        self.assertIsNone(self.store(settings).checkpoint("vitamin C"))
        second = FakeClient([])
        report = self.run_job(second, settings=settings)
        self.assertEqual(second.calls, [])
        self.assertEqual(report["fetched"], 0)
        self.assertNotEqual(report["status"], "COMPLETED")

    def test_report_contains_no_token_or_raw_text(self):
        self.keywords("collagen")
        report = self.run_job(FakeClient([page([fixture_post()])]))
        encoded = json.dumps(report, ensure_ascii=False)
        self.assertNotIn(SECRET, encoded)
        self.assertNotIn(TEXT, encoded)
        self.assertTrue(report.get("coverage_unknown"))
        self.assertTrue((self.root / "data" / "reports" / "threads_daily_latest.json").exists())


class RecoveryAndRetentionTests(CollectorCase):
    def test_each_crash_window_recovers_one_effective_event(self):
        self.keywords("collagen")
        for hook_stage in ["after_state", "after_raw_append", "after_event_append"]:
            with self.subTest(stage=hook_stage), tempfile.TemporaryDirectory(prefix="threads-crash-offline-") as tmp:
                root = Path(tmp)
                (root / "config").mkdir()
                (root / "config" / "threads_keywords.json").write_text(json.dumps({"keywords": ["collagen"]}))
                settings = m.Settings.from_env(root, environ=self.env)
                def crash(stage, *args):
                    if stage == hook_stage:
                        raise RuntimeError("SYNTHETIC CRASH")
                try:
                    m.run_collection(root=root, settings=settings, client=FakeClient([page([fixture_post()])]),
                                     now=NOW, crash_hook=crash)
                except RuntimeError:
                    pass
                else:
                    self.fail("expected crash hook to interrupt " + hook_stage)
                result = m.run_collection(root=root, settings=settings,
                                          client=FakeClient([page([fixture_post()])]), now=NOW)
                self.assertEqual(result["new"], 0)
                self.assertEqual(result["updated"], 0)
                rows = lines(root / "data" / "raw" / "threads_posts.jsonl")
                self.assertEqual(len({r["event_id"] for r in rows}), 1)
                events = [row for file in (root / "data" / "events").glob("*.jsonl") for row in lines(file)]
                self.assertEqual(len({e["event_id"] for e in events}), 1)
                self.assertEqual(len(rows), 1, "raw recovery should recognize already appended IDs")
                self.assertEqual(len(events), 1, "spool recovery should recognize already appended IDs")

    def test_partial_raw_and_event_tails_are_recovered_before_pending_delivery(self):
        store = self.store()
        store.ingest(fixture_post(), "collagen", NOW.isoformat())
        store.flush_outbox()
        store.ingest(fixture_post("synthetic-2", TEXT + " second"), "collagen", NOW.isoformat())
        raw = self.root / "data/raw/threads_posts.jsonl"
        event_file = next((self.root / "data/events").glob("*.jsonl"))
        for file in (raw, event_file):
            with file.open("ab") as stream:
                stream.write(b'{"event_id":"SYNTHETIC-incomplete')
        store.flush_outbox()
        self.assertEqual(len(lines(raw)), 2)
        self.assertEqual(len(lines(event_file)), 2)
        self.assertTrue(raw.read_bytes().endswith(b"\n"))
        self.assertTrue(event_file.read_bytes().endswith(b"\n"))

    def test_complete_malformed_jsonl_is_not_silently_rewritten(self):
        raw = self.root / "data/raw/threads_posts.jsonl"
        raw.parent.mkdir(parents=True)
        original = b'{invalid SYNTHETIC JSON}\n'
        raw.write_bytes(original)
        store = self.store()
        store.ingest(fixture_post(), "collagen", NOW.isoformat())
        with self.assertRaises(m.CollectorError) as caught:
            store.flush_outbox()
        self.assertEqual(caught.exception.status, "CORRUPT_STORAGE")
        self.assertEqual(raw.read_bytes(), original)

    def test_metadata_only_never_persists_text_even_in_sqlite(self):
        self.keywords("collagen")
        settings = self.settings(THREADS_STORAGE_PERMISSION_STATUS="metadata_only")
        self.run_job(FakeClient([page([fixture_post()])]), settings=settings)
        self.assertEqual(self.raw(), [])
        refs = self.raw(refs=True)
        self.assertEqual(len(refs), 1)
        for key in ("text", "raw_text", "summary"):
            self.assertNotIn(key, refs[0])
        for file in (self.root / "data").rglob("*"):
            if file.is_file():
                self.assertNotIn(TEXT.encode(), file.read_bytes(), "raw text leaked into " + str(file))

    def test_suppression_excludes_all_versions_and_blocks_reingestion(self):
        settings = self.settings()
        store = self.store(settings)
        store.ingest(fixture_post(), "collagen", NOW.isoformat())
        store.flush_outbox()
        store.suppress("synthetic-1")
        status = store.ingest(fixture_post(text=TEXT + " changed"), "collagen", NOW.isoformat())
        self.assertEqual(status, "rejected")
        export = self.root / "export.jsonl"
        store.export(export, include_text=True, now=NOW)
        self.assertEqual(lines(export), [])

    def test_export_excludes_expired_content_even_before_destructive_purge(self):
        store = self.store()
        old = (NOW - timedelta(days=40)).isoformat()
        store.ingest(fixture_post(), "collagen", old)
        store.flush_outbox()
        export = self.root / "expired-export.jsonl"
        store.export(export, include_text=True, now=NOW)
        self.assertEqual(lines(export), [])
        # Purging originals remains an explicit operation; the export gate is read-only.
        self.assertEqual(len(self.raw()), 1)

    def test_purge_then_reingest_keeps_monotonic_event_identity(self):
        store = self.store()
        old = (NOW - timedelta(days=40)).isoformat()
        store.ingest(fixture_post(), "collagen", old)
        store.flush_outbox()
        first_event_id = self.events()[0]["event_id"]
        store.purge_expired(NOW)
        outcome = store.ingest(fixture_post(text=TEXT + " fresh content"), "collagen", NOW.isoformat())
        store.flush_outbox()
        self.assertEqual(outcome, "updated")
        self.assertEqual([row["version"] for row in self.raw()], [2])
        self.assertEqual([row["source_version"] for row in self.events()], [2])
        self.assertEqual(self.events()[0]["event_type"], "threads.post.updated")
        self.assertNotEqual(self.events()[0]["event_id"], first_event_id)

    def test_nonempty_text_export_requires_redistribution_permission(self):
        store = self.store()
        store.ingest(fixture_post(), "collagen", NOW.isoformat())
        store.flush_outbox()
        destination = self.root / "unauthorized-text-export.jsonl"
        with self.assertRaises(m.CollectorError) as caught:
            store.export(destination, include_text=True, now=NOW)
        self.assertEqual(caught.exception.status, "BLOCKED_POLICY")
        self.assertFalse(destination.exists())
        references = self.root / "allowed-refs-export.jsonl"
        store.export(references, now=NOW)
        self.assertEqual(len(lines(references)), 1)
        self.assertNotIn("raw_text", lines(references)[0])

    def test_suppression_tombstone_survives_cleanup_crash_before_outbox_delivery(self):
        store = self.store()
        store.ingest(fixture_post(), "collagen", NOW.isoformat())
        with patch.object(store, "_remove_payloads", side_effect=RuntimeError("SYNTHETIC CLEANUP CRASH")):
            with self.assertRaises(RuntimeError):
                store.suppress("synthetic-1")
        store.flush_outbox()
        self.assertEqual(self.raw(), [])
        self.assertEqual(self.events(), [])
        destination = self.root / "suppressed-export.jsonl"
        store.export(destination, now=NOW)
        self.assertEqual(lines(destination), [])

    def test_purge_tombstone_recovers_cleanup_crash_before_pending_delivery(self):
        store = self.store()
        old = (NOW - timedelta(days=40)).isoformat()
        store.ingest(fixture_post(), "collagen", old)
        with patch.object(store, "_remove_payloads", side_effect=RuntimeError("SYNTHETIC PURGE CRASH")):
            with self.assertRaises(RuntimeError):
                store.purge_expired(NOW)
        destination = self.root / "pending-purge-export.jsonl"
        store.export(destination, now=NOW)
        self.assertEqual(lines(destination), [])
        store.flush_outbox()
        self.assertEqual(self.raw(), [])
        self.assertEqual(self.events(), [])
        self.assertNotIn(TEXT, "\n".join(store.db.iterdump()))

    def test_retention_expiry_removes_export_and_purges_payload(self):
        store = self.store()
        old = (NOW - timedelta(days=40)).isoformat()
        store.ingest(fixture_post(), "collagen", old)
        store.flush_outbox()
        store.purge_expired(NOW)
        export = self.root / "export.jsonl"
        store.export(export, include_text=True, now=NOW)
        self.assertEqual(lines(export), [])
        self.assertNotIn(TEXT, "\n".join(store.db.iterdump()))


class HTTPTests(CollectorCase):
    def test_oauth_error_400_is_permission_block_without_logging_body(self):
        body = json.dumps({'error': {'code': 190, 'message': SECRET + TEXT}}).encode()
        error = HTTPError('https://graph.threads.net/keyword_search', 400, 'OAuth', {}, io.BytesIO(body))
        with patch.object(m, 'urlopen', side_effect=error) as transport:
            with self.assertRaises(m.CollectorError) as caught:
                self.client().search({'q': 'collagen'})
        self.assertEqual(caught.exception.status, 'BLOCKED_PERMISSION')
        self.assertNotIn(SECRET, str(caught.exception))
        self.assertEqual(transport.call_count, 1)

    def client(self):
        self.sleeps = []
        return m.ThreadsClient(self.settings(), sleep=self.sleeps.append, random_fn=lambda: 0.0)

    def http_error(self, code, retry_after=None):
        headers = {"Retry-After": retry_after} if retry_after is not None else {}
        return HTTPError("https://graph.threads.net/keyword_search", code, "SYNTHETIC TEST", headers,
                         io.BytesIO((SECRET + TEXT).encode()))

    def test_authorization_is_bearer_header_never_query_parameter(self):
        client = self.client()
        with patch.object(m, "urlopen", return_value=Response(page())) as transport:
            client.search({"q": "collagen", "search_type": "RECENT", "limit": 25})
        request = transport.call_args.args[0]
        self.assertNotIn(SECRET, request.full_url)
        self.assertNotIn("access_token", request.full_url)
        self.assertEqual(request.get_header("Authorization"), "Bearer " + SECRET)

    def test_401_and_403_stop_without_retry_or_sensitive_error_body(self):
        for code in (401, 403):
            with self.subTest(code=code):
                client = self.client()
                with patch.object(m, "urlopen", side_effect=self.http_error(code)) as transport:
                    with self.assertRaises(m.CollectorError) as caught:
                        client.search({"q": "collagen"})
                self.assertEqual(transport.call_count, 1)
                self.assertEqual(caught.exception.status, "BLOCKED_PERMISSION")
                self.assertNotIn(SECRET, str(caught.exception))
                self.assertNotIn(TEXT, str(caught.exception))

    def test_429_seconds_stops_and_exposes_retry_after_without_retry(self):
        client = self.client()
        with patch.object(m, "urlopen", side_effect=self.http_error(429, "120")) as transport:
            with self.assertRaises(m.CollectorError) as caught:
                client.search({"q": "collagen"})
        self.assertEqual(transport.call_count, 1)
        self.assertEqual(caught.exception.status, "THROTTLED")
        self.assertEqual(caught.exception.retry_after, 120)

    def test_429_http_date_parses_retry_after(self):
        client = self.client()
        future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=120), usegmt=True)
        with patch.object(m, "urlopen", side_effect=self.http_error(429, future)):
            with self.assertRaises(m.CollectorError) as caught:
                client.search({"q": "collagen"})
        self.assertGreater(caught.exception.retry_after, 115)
        self.assertLessEqual(caught.exception.retry_after, 121)

    def test_500_and_timeout_retry_then_recover(self):
        for failure in (self.http_error(500), TimeoutError("SYNTHETIC timeout"), URLError("SYNTHETIC reset")):
            with self.subTest(failure=type(failure).__name__):
                client = self.client()
                with patch.object(m, "urlopen", side_effect=[failure, Response(page())]) as transport:
                    result = client.search({"q": "collagen"})
                self.assertEqual(result, page())
                self.assertEqual(transport.call_count, 2)
                self.assertGreaterEqual(len(self.sleeps), 1)

    def test_500_retry_is_bounded(self):
        client = self.client()
        with patch.object(m, "urlopen", side_effect=self.http_error(500)) as transport:
            with self.assertRaises(m.CollectorError):
                client.search({"q": "collagen"})
        self.assertLessEqual(transport.call_count, 5)
        self.assertGreater(transport.call_count, 1)

    def test_malformed_data_and_json_are_controlled_errors(self):
        for payload in ({"data": "wrong"}, {"unexpected": []}, b'{not json', {"data": ["wrong"]}):
            with self.subTest(payload=payload):
                client = self.client()
                with patch.object(m, "urlopen", return_value=Response(payload)):
                    with self.assertRaises(m.CollectorError):
                        client.search({"q": "collagen"})

    def test_untrusted_paging_next_never_becomes_request_url(self):
        self.keywords("collagen")
        first = page([fixture_post()], "cursor-2")
        first["paging"]["next"] = "https://evil.example/steal?access_token=SYNTHETIC"
        settings = self.settings()
        client = m.ThreadsClient(settings, sleep=lambda seconds: None, random_fn=lambda: 0.0)
        with patch.object(m, "urlopen", side_effect=[Response(first), Response(page())]) as transport:
            report = self.run_job(client, settings=settings)
        self.assertEqual(report["new"], 1)
        self.assertEqual(transport.call_count, 2)
        for call in transport.call_args_list:
            request = call.args[0]
            self.assertTrue(request.full_url.startswith(settings.api_base + "/keyword_search?"))
            self.assertNotIn("evil.example", request.full_url)
            self.assertNotIn(SECRET, request.full_url)
        self.assertIn("after=cursor-2", transport.call_args_list[1].args[0].full_url)

    def test_each_http_retry_consumes_persistent_daily_budget(self):
        self.keywords("collagen")
        settings = self.settings(THREADS_DAILY_MAX_REQUESTS="2")
        client = m.ThreadsClient(settings, sleep=lambda seconds: None, random_fn=lambda: 0.0)
        with patch.object(m, "urlopen", side_effect=self.http_error(500)) as transport:
            report = self.run_job(client, settings=settings)
        self.assertEqual(transport.call_count, 2)
        self.assertEqual(report["requests"], 2)
        self.assertEqual(report["status"], "QUOTA_EXHAUSTED")
        self.assertEqual(report["fetched"], 0)
        next_client = m.ThreadsClient(settings, sleep=lambda seconds: None, random_fn=lambda: 0.0)
        with patch.object(m, "urlopen", side_effect=AssertionError("budget must block transport")):
            next_report = self.run_job(next_client, settings=settings)
        self.assertEqual(next_report["status"], "QUOTA_EXHAUSTED")
        self.assertEqual(next_report["requests"], 0)

    def test_invalid_field_400_stops_without_retry(self):
        client = self.client()
        with patch.object(m, "urlopen", side_effect=self.http_error(400)) as transport:
            with self.assertRaises(m.CollectorError):
                client.search({"q": "collagen", "fields": "unknown_field"})
        self.assertEqual(transport.call_count, 1)

    def test_unknown_response_fields_do_not_reject_supported_contract(self):
        client = self.client()
        response = {"data": [fixture_post(extra="ignored")], "future_extra_field": True}
        with patch.object(m, "urlopen", return_value=Response(response)):
            self.assertEqual(client.search({"q": "collagen"})["data"][0]["id"], "synthetic-1")


if __name__ == "__main__":
    unittest.main()
