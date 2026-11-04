"""SYNTHETIC OFFLINE TEST ONLY: classification and local claim exports."""
from contextlib import closing
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from claim_baseline_se363 import process
from ingestion.threads_role_classifier import classify_post


class RoleTests(unittest.TestCase):
    def test_roles_are_candidates_requiring_human_review(self):
        samples = {
            'Shop mình ưu đãi vitamin C, đặt hàng ngay.': 'brand_promotional_post',
            'Collagen, link trong bio của mình.': 'affiliate_promotion_candidate',
            'Mình đã dùng vitamin C, đây là trải nghiệm của mình.': 'user_review',
            'Mọi người có nên uống vitamin C không?': 'discussion',
            'Vitamin C': 'unknown',
        }
        for text, role in samples.items():
            with self.subTest(text=text):
                result = classify_post(text, ['vitamin C'])
                self.assertEqual(result['post_role'], role)
                self.assertTrue(result['needs_human_review'])
                self.assertEqual(result['provenance']['search_queries'], ['vitamin C'])
                self.assertNotIn('fraud', result)

    def test_sales_language_alone_does_not_establish_brand_ownership(self):
        self.assertEqual(classify_post('Mua ngay vitamin C, freeship')['post_role'], 'unknown')

    def test_configured_noise_stays_reviewable_without_excluding_raw_post(self):
        result = classify_post('Shop mình tuyển nhân viên, ưu đãi vitamin C, đặt hàng.',
                               ['vitamin C'], noise_terms=['tuyển nhân viên'])
        self.assertEqual(result['post_role'], 'unknown')
        self.assertEqual(result['provenance']['matched_noise_terms'], ['tuyển nhân viên'])
        self.assertTrue(result['needs_human_review'])


class ClaimTests(unittest.TestCase):
    def test_quantity_claim_keeps_decimals_percentages_and_exact_spans(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-quantity-claim-') as directory:
            root = Path(directory)
            source = root / 'posts.jsonl'
            text = '  Mỗi viên 1.5 mg.\nĐộ tinh khiết 10%. Hàm lượng 2,5 g.'
            source.write_text(json.dumps({'post_id': 'RSS-QUANTITY', 'raw_text': text,
                                         'source_role': 'promotional_content_candidate'}) + '\n')
            report = process(source, root / 'output', root / 'reports')
            self.assertEqual(report['claims_for_manual_review'], 3)
            with (root / 'output/claim_candidates_review.csv').open(encoding='utf-8-sig') as stream:
                claims = list(csv.DictReader(stream))
            self.assertEqual([row['claim_text'] for row in claims],
                             ['Mỗi viên 1.5 mg.', 'Độ tinh khiết 10%.', 'Hàm lượng 2,5 g.'])
            for row in claims:
                self.assertEqual(text[int(row['span_start']):int(row['span_end'])], row['claim_text'])
                self.assertEqual(row['claim_category'], 'dinh_luong')
                self.assertEqual(row['verification_label'], '')

    def test_latest_rss_snapshot_replaces_stale_claim_candidates(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-rss-versions-') as directory:
            root = Path(directory)
            source = root / 'posts.jsonl'
            common = {'post_id': 'RSS-VERSIONED', 'source_role': 'promotional_content_candidate',
                      'source_type': 'publisher_store_rss_candidate'}
            rows = [
                {**common, '_key': 'legacy', 'source_url': 'https://example.invalid/item?utm_source=test',
                 'raw_text': 'Chứa vitamin C.'},
                {**common, '_key': 'new-snapshot', 'source_record_id': 'rss:synthetic', 'version': 2,
                 'source_url': 'https://example.invalid/item', 'raw_text': 'Chứa 2.5 mg canxi.'},
            ]
            original = ''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows)
            source.write_text(original, encoding='utf-8')
            report = process(source, root / 'output', root / 'reports')
            self.assertEqual(report['posts_for_manual_review'], 1)
            self.assertEqual(report['claims_for_manual_review'], 1)
            self.assertEqual(report['skipped_old_rss_versions'], 1)
            self.assertEqual(source.read_text(encoding='utf-8'), original)
            with (root / 'output/claim_candidates_review.csv').open(encoding='utf-8-sig') as stream:
                claims = list(csv.DictReader(stream))
            self.assertEqual(claims[0]['claim_text'], 'Chứa 2.5 mg canxi.')
            self.assertEqual(claims[0]['source_version'], '2')

    def test_threads_claim_export_shares_suppression_lock(self):
        from ingestion.threads_collector import CollectorError, collector_lock
        with tempfile.TemporaryDirectory(prefix='synthetic-claim-lock-') as directory:
            root = Path(directory)
            source = root / 'threads_posts.jsonl'
            source.write_text(json.dumps({'source_platform': 'threads', 'post_id': 'SYNTHETIC',
                                         'raw_text': 'Chứa vitamin C.'}) + '\n')
            with collector_lock(root), self.assertRaises(CollectorError) as caught:
                process(source, root / 'output', root / 'reports')
            self.assertEqual(caught.exception.status, 'BUSY')
            self.assertFalse((root / 'output').exists())

    def test_latest_threads_text_refs_and_suppression(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-claim-') as directory:
            root = Path(directory)
            source = root / 'threads_posts.jsonl'
            common = {'source_platform': 'threads', 'source_role': 'threads_post_candidate',
                      'permalink': 'https://example.invalid/synthetic', 'observed_at': '2026-11-01T00:00:00+00:00'}
            records = [
                {**common, 'post_id': 'SYNTHETIC-1', 'version': 1, 'raw_text': 'Chứa vitamin C.'},
                {**common, 'post_id': 'SYNTHETIC-1', 'version': 2, 'raw_text': 'Hỗ trợ miễn dịch.'},
                {**common, 'post_id': 'SYNTHETIC-REF', 'version': 1},
                {**common, 'post_id': 'SYNTHETIC-REMOVED', 'version': 1, 'raw_text': 'Chứa canxi.'},
            ]
            source.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in records), encoding='utf-8')
            state = root / 'state.sqlite'
            with closing(sqlite3.connect(state)) as db, db:
                db.execute('CREATE TABLE suppressions (post_id TEXT PRIMARY KEY, suppressed_at TEXT)')
                db.execute('INSERT INTO suppressions VALUES (?,?)', ('SYNTHETIC-REMOVED', '2026-11-03'))
            report = process(source, root / 'output', root / 'reports', state,
                             now=datetime(2026, 11, 3, tzinfo=timezone.utc), retention_days=30)
            self.assertEqual(report['posts_for_manual_review'], 1)
            self.assertEqual(report['claims_for_manual_review'], 1)
            self.assertEqual(report['skipped_suppressed_records'], 1)
            self.assertEqual(report['gold_claims'], 0)
            with (root / 'output/claim_candidates_review.csv').open(encoding='utf-8-sig') as stream:
                claims = list(csv.DictReader(stream))
            self.assertEqual(claims[0]['claim_text'], 'Hỗ trợ miễn dịch.')
            self.assertEqual(claims[0]['source_version'], '2')
            self.assertEqual(claims[0]['claim_kind'], 'claim_candidate')
            self.assertEqual(claims[0]['verification_label'], '')
            self.assertEqual(claims[0]['source_url'], 'https://example.invalid/synthetic')

    def test_expired_latest_version_never_falls_back_to_old_text(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-expired-claim-') as directory:
            root = Path(directory)
            source = root / 'threads_posts.jsonl'
            common = {'source_platform': 'threads', 'source_role': 'threads_post_candidate',
                      'post_id': 'SYNTHETIC-1', 'raw_text': 'Chứa vitamin C.'}
            rows = [
                {**common, 'version': 1, 'observed_at': '2026-11-03T00:00:00+00:00'},
                {**common, 'version': 2, 'observed_at': '2026-09-01T00:00:00+00:00'},
            ]
            source.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')
            report = process(source, root / 'output', root / 'reports',
                             now=datetime(2026, 11, 3, tzinfo=timezone.utc), retention_days=30)
            self.assertEqual(report['claims_for_manual_review'], 0)
            self.assertEqual(report['skipped_expired_threads_records'], 1)

    def test_missing_observed_time_fails_closed_for_threads(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-unknown-time-') as directory:
            root = Path(directory)
            source = root / 'threads_posts.jsonl'
            source.write_text(json.dumps({'source_platform': 'threads', 'source_role': 'threads_post_candidate',
                                          'post_id': 'SYNTHETIC-1', 'raw_text': 'Chứa vitamin C.'}) + '\n')
            report = process(source, root / 'output', root / 'reports', retention_days=30)
            self.assertEqual(report['claims_for_manual_review'], 0)
            self.assertEqual(report['skipped_expired_threads_records'], 1)

    def test_legacy_role_gate_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix='synthetic-legacy-claim-') as directory:
            root = Path(directory)
            source = root / 'posts.jsonl'
            rows = [
                {'post_id': 'RSS-TEST', 'raw_text': 'Chứa vitamin C.', 'source_role': 'promotional_content_candidate'},
                {'post_id': 'EVIDENCE-TEST', 'raw_text': 'Hỗ trợ sức khỏe.', 'source_role': 'evidence_candidate'},
            ]
            source.write_text(''.join(json.dumps(row, ensure_ascii=False) + '\n' for row in rows), encoding='utf-8')
            report = process(source, root / 'output', root / 'reports')
            self.assertEqual(report['posts_for_manual_review'], 1)
            self.assertEqual(report['claims_for_manual_review'], 1)
            self.assertEqual(report['skipped_non_promo_roles'], 1)


if __name__ == '__main__':
    unittest.main()
