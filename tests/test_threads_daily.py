"""SYNTHETIC OFFLINE TEST ONLY: daily integration without real data or network."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import daily_se363 as daily
from ingestion import threads_collector as collector
from ingestion.threads_collector import CollectorError, collector_lock


def write_row(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as out:
        out.write(json.dumps(row, ensure_ascii=False) + '\n')


def thread(version=1, **extra):
    return {
        'post_id': 'SYNTHETIC-1', 'source_platform': 'threads',
        'source_role': 'threads_post_candidate', 'version': version,
        'raw_text': 'SYNTHETIC OFFLINE TEST ONLY vitamin C',
        'published_at': '2026-11-01T01:00:00+00:00',
        'observed_at': '2026-11-02T01:00:00+00:00',
        'permalink': 'https://example.invalid/synthetic',
        **extra,
    }


class ThreadsSpoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='synthetic-threads-daily-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = self.root / 'raw'
        self.raw.mkdir()
        self.state = self.root / 'state.json'
        self.events = self.root / 'events'

    def test_upgrade_preserves_legacy_offsets_and_baselines_threads(self):
        old = {'posts_auto.jsonl': 37, 'evidence_discovery.jsonl': 42, 'product_metadata.jsonl': 13}
        self.state.write_text(json.dumps({'offsets': old, 'created_at': 'unchanged'}))
        write_row(self.raw / 'threads_posts.jsonl', thread())
        migrated = daily.initialize_offsets(self.state, self.raw)
        for key, value in old.items():
            self.assertEqual(migrated['offsets'][key], value)
        self.assertEqual(migrated['created_at'], 'unchanged')
        self.assertEqual(migrated['offsets']['threads_posts.jsonl'], (self.raw / 'threads_posts.jsonl').stat().st_size)
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)

    def test_stable_events_and_metadata_only_payload(self):
        daily.initialize_offsets(self.state, self.raw)
        source = self.raw / 'threads_posts.jsonl'
        write_row(source, thread())
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 1)
        write_row(source, thread(observed_at='2026-11-03T00:00:00+00:00'))
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)
        write_row(source, thread(version=2, raw_text='SYNTHETIC EDIT'))
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 1)
        rows = [json.loads(line) for path in self.events.glob('*.jsonl') for line in path.read_text().splitlines()]
        self.assertEqual([r['event_type'] for r in rows], ['threads.post.discovered', 'threads.post.updated'])
        self.assertEqual(rows[0]['event_id'], hashlib.sha256(b'threads|SYNTHETIC-1|1|threads.post.discovered').hexdigest())
        self.assertEqual(rows[1]['source_version'], 2)
        self.assertFalse(rows[0]['is_replay'])
        self.assertNotIn('raw_text', rows[0]['payload'])

    def test_collector_event_from_previous_day_is_not_duplicated(self):
        daily.initialize_offsets(self.state, self.raw)
        row = thread()
        write_row(self.raw / 'threads_posts.jsonl', row)
        write_row(self.events / 'threads_events_20000101.jsonl', {
            'event_type': 'threads.post.discovered',
            'event_id': daily.stable_record_id(row, 'threads_post_candidate'),
        })
        result = daily.emit_new_events(self.raw, self.state, self.events)
        self.assertEqual(result['events_new'], 0)
        state = json.loads(self.state.read_text())
        self.assertEqual(state['offsets']['threads_posts.jsonl'], (self.raw / 'threads_posts.jsonl').stat().st_size)

    def test_backfill_row_keeps_replay_flag(self):
        daily.initialize_offsets(self.state, self.raw)
        write_row(self.raw / 'threads_post_refs.jsonl', thread(historical_replay=True))
        daily.emit_new_events(self.raw, self.state, self.events)
        row = next(json.loads(line) for p in self.events.glob('*.jsonl') for line in p.read_text().splitlines())
        self.assertTrue(row['is_replay'])
        self.assertTrue(row['historical_replay'])

    def test_incomplete_json_line_does_not_advance_offset(self):
        daily.initialize_offsets(self.state, self.raw)
        source = self.raw / 'threads_posts.jsonl'
        source.write_text(json.dumps(thread()), encoding='utf-8')
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)
        self.assertEqual(json.loads(self.state.read_text())['offsets']['threads_posts.jsonl'], 0)
        with source.open('a') as stream:
            stream.write('\n')
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 1)

    def test_suppressed_threads_do_not_escape_pending_raw(self):
        daily.initialize_offsets(self.state, self.raw)
        write_row(self.raw / 'threads_posts.jsonl', thread())
        with closing(sqlite3.connect(self.state.parent / 'threads_collector.sqlite')) as db, db:
            db.execute('CREATE TABLE suppressions (post_id TEXT PRIMARY KEY, suppressed_at TEXT)')
            db.execute('INSERT INTO suppressions VALUES (?, ?)', ('SYNTHETIC-1', '2026-11-03'))
        for replay in (False, True):
            with self.subTest(historical_replay=replay):
                self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events,
                                                       historical_replay=replay)['events_new'], 0)

    def test_crash_tail_recovered_before_new_event(self):
        daily.initialize_offsets(self.state, self.raw)
        write_row(self.raw / 'threads_posts.jsonl', thread())
        self.events.mkdir()
        dest = self.events / ('new_events_' + daily.datetime.now().strftime('%Y%m%d') + '.jsonl')
        dest.write_text('{"event_id":"interrupted', encoding='utf-8')
        result = daily.emit_new_events(self.raw, self.state, self.events)
        self.assertEqual(result['events_new'], 1)
        self.assertTrue(result['warnings'])
        self.assertEqual(len(dest.read_text().splitlines()), 1)
        self.assertEqual(json.loads(dest.read_text())['post_id'], 'SYNTHETIC-1')
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)

    def test_busy_collector_does_not_advance_threads_offset(self):
        daily.initialize_offsets(self.state, self.raw)
        write_row(self.raw / 'threads_posts.jsonl', thread())
        with collector_lock(self.root):
            result = daily.emit_new_events(self.raw, self.state, self.events)
        self.assertEqual(result['events_new'], 0)
        self.assertTrue(any('BUSY' in warning for warning in result['warnings']))
        self.assertEqual(json.loads(self.state.read_text())['offsets']['threads_posts.jsonl'], 0)

    def test_legacy_crash_retry_after_midnight_does_not_reemit(self):
        class Clock(datetime):
            day = 3

            @classmethod
            def now(cls, tz=None):
                return cls(2026, 11, cls.day, 23, 59, tzinfo=tz)

        daily.initialize_offsets(self.state, self.raw)
        for name in ('posts_auto.jsonl', 'evidence_discovery.jsonl', 'product_metadata.jsonl'):
            write_row(self.raw / name, {'_key': 'SYNTHETIC-' + name, 'raw_text': 'OFFLINE ONLY'})
        with patch.object(daily, 'datetime', Clock):
            # The first event is durable, but its source cursor is still zero.
            with patch.object(daily, 'atomic_json', side_effect=OSError('SYNTHETIC CRASH')):
                with self.assertRaises(OSError):
                    daily.emit_new_events(self.raw, self.state, self.events)
            Clock.day = 4
            recovered = daily.emit_new_events(self.raw, self.state, self.events)
        rows = [json.loads(line) for path in self.events.glob('*.jsonl') for line in path.read_text().splitlines()]
        self.assertEqual(recovered['events_new'], 2)
        self.assertEqual(len(rows), 3)
        self.assertEqual(len({row['event_id'] for row in rows}), 3)
        self.assertTrue(all(not row['historical_replay'] for row in rows))

    def test_busy_bootstrap_preserves_offsets_and_next_daily_does_not_replay(self):
        write_row(self.raw / 'posts_auto.jsonl', {'_key': 'SYNTHETIC-HISTORY', 'raw_text': 'OFFLINE ONLY'})
        daily.initialize_offsets(self.state, self.raw)
        before = self.state.read_bytes()
        with collector_lock(self.root), \
             patch.multiple(daily, RAW=self.raw, STATE=self.state, EVENTS=self.events), \
             patch('sys.argv', ['daily_se363.py', '--bootstrap-existing']), \
             patch('sys.stdout', new_callable=io.StringIO) as output:
            daily.main()
        self.assertIn('BUSY', output.getvalue())
        self.assertEqual(self.state.read_bytes(), before)
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)

    def test_successful_bootstrap_still_marks_existing_history_as_replay(self):
        write_row(self.raw / 'posts_auto.jsonl', {'_key': 'SYNTHETIC-HISTORY', 'raw_text': 'OFFLINE ONLY'})
        daily.initialize_offsets(self.state, self.raw)
        result = daily.emit_new_events(self.raw, self.state, self.events, historical_replay=True)
        rows = [json.loads(line) for path in self.events.glob('*.jsonl') for line in path.read_text().splitlines()]
        self.assertEqual(result['events_new'], 1)
        self.assertTrue(rows[0]['historical_replay'])

    def test_replay_crash_after_append_never_rewinds_normal_offsets(self):
        for name in ('posts_auto.jsonl', 'evidence_discovery.jsonl'):
            write_row(self.raw / name, {'_key': 'SYNTHETIC-HISTORY-' + name, 'raw_text': 'OFFLINE ONLY'})
        daily.initialize_offsets(self.state, self.raw)
        before = self.state.read_bytes()
        replay_state = self.state.with_name(self.state.stem + '_replay' + self.state.suffix)
        atomic_json = daily.atomic_json

        def interrupt_checkpoint(path, value):
            if path == replay_state and any(self.events.glob('*.jsonl')):
                raise OSError('SYNTHETIC CRASH after replay event fsync')
            return atomic_json(path, value)

        with patch.object(daily, 'atomic_json', side_effect=interrupt_checkpoint):
            with self.assertRaisesRegex(OSError, 'SYNTHETIC CRASH'):
                daily.emit_new_events(self.raw, self.state, self.events, historical_replay=True)
        self.assertEqual(self.state.read_bytes(), before)
        replayed = [json.loads(line) for path in self.events.glob('*.jsonl') for line in path.read_text().splitlines()]
        self.assertEqual(len(replayed), 1)
        self.assertTrue(replayed[0]['historical_replay'])
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)
        self.assertEqual(self.state.read_bytes(), before)

    def test_replay_before_first_normal_run_creates_a_separate_normal_baseline(self):
        source = self.raw / 'posts_auto.jsonl'
        write_row(source, {'_key': 'SYNTHETIC-HISTORY', 'raw_text': 'OFFLINE ONLY'})
        result = daily.emit_new_events(self.raw, self.state, self.events, historical_replay=True)
        self.assertEqual(result['events_new'], 1)
        normal = json.loads(self.state.read_text())
        self.assertEqual(normal['offsets']['posts_auto.jsonl'], source.stat().st_size)
        before = self.state.read_bytes()
        self.assertEqual(daily.emit_new_events(self.raw, self.state, self.events)['events_new'], 0)
        self.assertEqual(self.state.read_bytes(), before)


class ThreadsDailyStageTests(unittest.TestCase):
    def test_default_daily_keeps_rss_optional_and_ignores_its_stale_errors(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-threads-primary-') as directory:
            root = Path(directory)
            raw, reports = root / 'data/raw', root / 'data/reports'
            raw.mkdir(parents=True)
            reports.mkdir(parents=True)
            for name in ('expand_se363.py', 'claim_baseline_se363.py'):
                (root / name).touch()
            write_row(raw / 'posts_auto.jsonl', {'_key': 'SYNTHETIC-HISTORY', 'raw_text': 'OFFLINE ONLY'})
            (reports / 'rss_expansion_report.json').write_text(json.dumps({'errors': ['OLD RSS FAILURE']}))
            with patch.multiple(daily, ROOT=root, RAW=raw, REPORTS=reports,
                                EVENTS=root/'data/events', LOGS=root/'data/logs', STATE=root/'data/state/daily_offsets.json'), \
                 patch.object(collector, 'run_collection', return_value={'status': 'COMPLETED'}), \
                 patch.object(daily, 'run_command', return_value={'ok': True, 'exit_code': 0}) as command, \
                 patch.object(daily, 'read_configured_brands', return_value=False), \
                 patch.object(daily, 'read_meta_enabled', return_value=False):
                self.assertEqual(daily.daily_once(), 0)
            result = json.loads((reports / 'daily_latest.json').read_text())
            self.assertEqual(result['status'], 'OK')
            self.assertIn('skipped', result['stages']['rss'])
            self.assertNotIn('claim_baseline', result['stages'])
            self.assertEqual(result['warnings'], [])
            self.assertEqual([call.args[1] for call in command.call_args_list], [['expand_se363.py', 'report']])

    def test_total_events_includes_new_and_recovered_outbox_without_double_counting(self):
        now = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
        settings = collector.Settings(
            token='SYNTHETIC OFFLINE TOKEN', enable_daily=True,
            collection_permission_status='verified', storage_permission_status='metadata_only',
            verified_scopes=('threads_basic', 'threads_keyword_search'), policy_reviewed_at='2026-11-02',
        )

        def post(pid):
            return {'id': pid, 'text': 'SYNTHETIC OFFLINE collagen',
                    'timestamp': '2026-11-03T01:00:00+00:00',
                    'permalink': 'https://www.threads.net/@synthetic/post/' + pid}

        with tempfile.TemporaryDirectory(prefix='synthetic-daily-outbox-') as directory:
            root = Path(directory)
            raw = root / 'data/raw'
            raw.mkdir(parents=True)
            (root / 'config').mkdir()
            (root / 'config/threads_keywords.json').write_text(json.dumps({'keywords': ['collagen']}))
            for name in ('expand_se363.py', 'claim_baseline_se363.py'):
                (root / name).touch()
            # Recover pending state from a previous run even though this run's
            # `new` count only includes the API's newly discovered post.
            with collector.Store(root, settings) as store:
                store.ingest(post('SYNTHETIC-ALREADY-WRITTEN'), 'collagen', now.isoformat())
                store.flush_outbox()
                # Simulate crash after event fsync but before its SQL delivery bit.
                with store.db:
                    store.db.execute('UPDATE outbox SET event_done=0')
                store.ingest(post('SYNTHETIC-RECOVERED'), 'collagen', now.isoformat())
                # Pending suppression/purge cleanup must not count as delivery.
                store.ingest(post('SYNTHETIC-SUPPRESSED'), 'collagen', now.isoformat())
                store.ingest(post('SYNTHETIC-PURGED'), 'collagen', now.isoformat())
                with store.db:
                    store.db.execute('INSERT INTO suppressions VALUES (?, ?)', ('SYNTHETIC-SUPPRESSED', now.isoformat()))
                    store.db.execute('INSERT INTO purged_versions VALUES (?, ?)', ('SYNTHETIC-PURGED', 1))
            collect = collector.run_collection

            def run_threads(**kwargs):
                client = SimpleNamespace(search=lambda params: {'data': [post('SYNTHETIC-NEW')]})
                return collect(**kwargs, settings=settings, now=now, client=client)

            def run_command(label, command, log):
                if command[:2] == ['expand_se363.py', 'rss']:
                    write_row(raw / 'posts_auto.jsonl', {'_key': 'SYNTHETIC-RSS', 'raw_text': 'OFFLINE ONLY'})
                return {'ok': True, 'exit_code': 0}

            with patch.multiple(daily, ROOT=root, RAW=raw, REPORTS=root/'data/reports',
                                EVENTS=root/'data/events', LOGS=root/'data/logs', STATE=root/'data/state/daily_offsets.json'), \
                 patch.object(collector, 'run_collection', side_effect=run_threads), \
                 patch.object(daily, 'run_command', side_effect=run_command), \
                 patch.object(daily, 'read_configured_brands', return_value=False), \
                 patch.object(daily, 'read_meta_enabled', return_value=False), \
                 patch('sys.stdout', new_callable=io.StringIO) as output:
                self.assertEqual(daily.daily_once(with_rss=True), 0)
                report = json.loads((root / 'data/reports/daily_latest.json').read_text())
                self.assertEqual(report['stages']['threads']['new'], 1)
                self.assertEqual(report['stages']['threads']['events_emitted'], 2)
                self.assertEqual(report['queued']['scanner_events_new'], 1)
                self.assertEqual(report['queued']['threads_outbox_events_new'], 2)
                self.assertEqual(report['queued']['events_new'], 3)
                self.assertEqual(report['queued']['by_role']['threads_post_candidate'], 2)
                self.assertEqual(len(report['queued']['spools']), 2)
                self.assertIn('events_new=3', output.getvalue())
                self.assertEqual(daily.daily_once(with_rss=True), 0)
                retry = json.loads((root / 'data/reports/daily_latest.json').read_text())
                self.assertEqual(retry['stages']['threads']['events_emitted'], 0)
                self.assertEqual(retry['queued']['events_new'], 0)
            events = [json.loads(line) for path in (root / 'data/events').glob('*.jsonl')
                      for line in path.read_text().splitlines()]
            self.assertEqual(len(events), 4)  # One existed before either daily run.
            self.assertEqual(len({event['event_id'] for event in events}), 4)

    def test_disabled_and_blocked_threads_allow_legacy_stages(self):
        for status in ('SKIPPED_CONFIG_DISABLED', 'BLOCKED_PERMISSION', 'BLOCKED_POLICY', 'PARTIAL', 'QUOTA_EXHAUSTED', 'BUSY'):
            with self.subTest(status=status), tempfile.TemporaryDirectory(prefix='synthetic-daily-stage-') as directory:
                root = Path(directory)
                for name in ('expand_se363.py', 'claim_baseline_se363.py'):
                    (root / name).touch()
                raw = root / 'data' / 'raw'
                raw.mkdir(parents=True)
                write_row(raw / 'posts_auto.jsonl', {'_key': 'SYNTHETIC', 'raw_text': 'SYNTHETIC OFFLINE TEST ONLY'})
                runner = SimpleNamespace(run_collection=lambda **kwargs: {'status': status, 'fetched': 0, 'new': 0, 'updated': 0},
                                         collector_lock=collector_lock, CollectorError=CollectorError)
                with patch.multiple(daily, ROOT=root, RAW=raw, REPORTS=root/'data/reports',
                                    EVENTS=root/'data/events', LOGS=root/'data/logs', STATE=root/'data/state/daily_offsets.json'), \
                     patch.dict('sys.modules', {'ingestion.threads_collector': runner}), \
                     patch.object(daily, 'run_command', return_value={'ok': True, 'exit_code': 0}) as command, \
                     patch.object(daily, 'read_configured_brands', return_value=False), \
                     patch.object(daily, 'read_meta_enabled', return_value=False):
                    self.assertEqual(daily.daily_once(with_rss=True), 0)
                    report = json.loads((root / 'data/reports/daily_latest.json').read_text())
                self.assertEqual(report['stages']['threads']['status'], status)
                self.assertEqual(report['stages']['threads']['fetched'], 0)
                self.assertTrue(report['stages']['rss']['ok'])
                self.assertEqual(report['status'], 'OK' if status == 'SKIPPED_CONFIG_DISABLED' else 'PARTIAL')
                self.assertTrue(any(call.args[1][0] == 'expand_se363.py' for call in command.call_args_list))


if __name__ == '__main__':
    unittest.main()
