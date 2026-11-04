"""Only synthetic fixtures; never contacts real sites."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest
import expand_se363 as m


class Tests(unittest.TestCase):
    def test_rss_xml_start_and_dedupe(self):
        x = '\n<?xml version="1.0"?><rss><channel><item><title>Demo</title><link>https://example.org/item?utm_source=x</link><description><![CDATA[<p>Chứa canxi</p>]]></description></item></channel></rss>'.encode()
        self.assertEqual(len(m.parse_rss(x)), 1)
        self.assertEqual(m.parse_rss(x)[0]['summary'], 'Chứa canxi')
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / 'raw'
            rep = base / 'reports'
            source = base / 'source.json'
            source.write_text(json.dumps([{
                'publisher': 'Example (test)', 'role': 'promotion_candidate',
                'url': 'https://example.org/test.rss',
                'authorized_for_collection': True, 'terms_url': 'https://example.org/terms'
            }]), encoding='utf-8')
            with patch.object(m, 'ROOT', base), patch.object(m, 'RAW', raw), patch.object(m, 'REPORTS', rep), \
                 patch.object(m, 'SOURCE_CONFIG', source), patch.object(m, 'request_bytes', return_value=x):
                r1 = m.collect_rss(sleep_seconds=0)
                r2 = m.collect_rss(sleep_seconds=0)
                self.assertEqual(r1['feeds'][0]['new'], 1)
                self.assertEqual(r2['feeds'][0]['new'], 0)
                self.assertEqual(len(list(m.load_jsonl(raw/'posts_auto.jsonl'))), 1)
                self.assertEqual(list(m.load_jsonl(raw/'posts_auto.jsonl'))[0]['source_role'], 'promotional_content_candidate')

    def test_rss_edits_append_versions_without_replaying_legacy_or_timestamps(self):
        feed = {'publisher': 'SYNTHETIC OFFLINE ONLY', 'role': 'promotion_candidate',
                'url': 'https://example.org/feed', 'authorized_for_collection': True,
                'terms_url': 'https://example.org/terms'}
        legacy = {'_key': 'legacy-key', 'post_id': 'RSS-legacy-identity',
                  'source_role': 'promotional_content_candidate',
                  'source_type': 'publisher_store_rss_candidate',
                  'source_url': 'https://example.org/item?utm_source=old',
                  'title': 'SYNTHETIC title', 'raw_text': 'SYNTHETIC hỗ trợ sức khỏe',
                  'observed_at': '2026-11-01T00:00:00Z'}
        def xml(title, summary, published):
            return (f'<rss><channel><item><title>{title}</title>'
                    f'<link>https://example.org/item?utm_source=new</link>'
                    f'<description>{summary}</description><pubDate>{published}</pubDate>'
                    '</item></channel></rss>').encode()
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / 'raw'
            raw.mkdir()
            dest = raw / 'posts_auto.jsonl'
            original = (json.dumps(legacy, ensure_ascii=False) + '\n').encode()
            dest.write_bytes(original)
            source = base / 'sources.json'
            source.write_text(json.dumps([feed]))
            payloads = [xml(legacy['title'], legacy['raw_text'], 'new timestamp'),
                        xml(legacy['title'], 'SYNTHETIC chữa bệnh', 'new timestamp'),
                        xml('SYNTHETIC title edited', 'SYNTHETIC chữa bệnh', 'another timestamp'),
                        xml('SYNTHETIC title edited', 'SYNTHETIC chữa bệnh', 'timestamp changed only')]
            with patch.object(m, 'ROOT', base), patch.object(m, 'RAW', raw), \
                 patch.object(m, 'REPORTS', base / 'reports'), patch.object(m, 'SOURCE_CONFIG', source), \
                 patch.object(m, 'request_bytes', side_effect=payloads):
                reports = [m.collect_rss(sleep_seconds=0) for _ in payloads]
            self.assertEqual([r['feeds'][0]['new'] for r in reports], [0, 1, 1, 0])
            rows = list(m.load_jsonl(dest))
            self.assertEqual(len(rows), 3)
            self.assertTrue(dest.read_bytes().startswith(original), 'legacy source bytes must stay unchanged')
            self.assertEqual([r['version'] for r in rows[1:]], [2, 3])
            self.assertEqual({r['post_id'] for r in rows}, {'RSS-legacy-identity'})
            self.assertEqual({r['source_record_id'] for r in rows[1:]}, {m.rss_identity(legacy)})
            self.assertNotEqual(rows[1]['content_hash_sha256'], rows[2]['content_hash_sha256'])

    def test_rss_evidence_versions_ignore_tracking_and_retry_unchanged_content(self):
        feed = {'role': 'evidence_candidate', 'publisher': 'SYNTHETIC OFFLINE ONLY',
                'url': 'https://example.org/feed', 'terms_url': 'https://example.org/terms',
                'authorized_for_collection': True}
        def xml(summary, tracking, published):
            return (f'<rss><channel><item><title>SYNTHETIC evidence</title>'
                    f'<link>https://example.org/article?utm_source={tracking}</link>'
                    f'<description>{summary}</description><pubDate>{published}</pubDate>'
                    '</item></channel></rss>').encode()
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            source = base / 'sources.json'
            source.write_text(json.dumps([feed]))
            payloads = [xml('SYNTHETIC summary', 'a', '2026-11-01'),
                        xml('SYNTHETIC summary', 'b', '2026-11-02'),
                        xml('SYNTHETIC corrected evidence', 'b', '2026-11-02'),
                        xml('SYNTHETIC summary', 'c', '2026-11-01')]
            with patch.object(m, 'ROOT', base), patch.object(m, 'RAW', base / 'raw'), \
                 patch.object(m, 'REPORTS', base / 'reports'), patch.object(m, 'SOURCE_CONFIG', source), \
                 patch.object(m, 'request_bytes', side_effect=payloads):
                reports = [m.collect_rss(sleep_seconds=0) for _ in payloads]
            self.assertEqual([r['feeds'][0]['new'] for r in reports], [1, 0, 1, 0])
            dest = base / 'raw/evidence_discovery.jsonl'
            rows = list(m.load_jsonl(dest))
            self.assertEqual([r['version'] for r in rows], [1, 2])
            self.assertEqual(len({r['source_record_id'] for r in rows}), 1)
            self.assertEqual(rows[-1]['summary'], 'SYNTHETIC corrected evidence')
            self.assertTrue(all(r['source_role'] == 'evidence_candidate' for r in rows))

    def test_role_report_uses_record_provenance_and_separates_versions(self):
        rss_post = {'source_role': 'promotional_content_candidate', 'source_type': 'publisher_store_rss_candidate',
                    'post_id': 'RSS-test', 'source_url': 'https://example.org/rss-item',
                    'title': 'SYNTHETIC', 'raw_text': 'SYNTHETIC summary'}
        other_post = {'source_role': 'advertisement_original', 'source_type': 'authorized_meta_ad_account', 'post_id': 'META-test'}
        brand = {'source_role': 'promotional_content_candidate', 'source_type': 'approved_brand_rss', 'post_id': 'FEED-test'}
        rss_evidence = {'source_role': 'evidence_candidate', 'source_type': 'publisher_rss_evidence_index', 'url': 'https://example.org/evidence'}
        gdelt = {'source_role': 'evidence_candidate', 'source_type': 'gdelt_news_index', 'url': 'https://example.org/news'}
        crossref = {'source_role': 'research_metadata', 'source_type': 'crossref', 'doi': '10.example/synthetic'}
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / 'raw'
            raw.mkdir()
            def write(name, rows):
                (raw / name).write_text(''.join(json.dumps(row) + '\n' for row in rows))
            write('posts_auto.jsonl', [rss_post, {**rss_post, 'raw_text': 'SYNTHETIC changed'}, other_post, brand])
            write('evidence_discovery.jsonl', [rss_evidence, gdelt])
            write('research_metadata.jsonl', [crossref])
            with patch.object(m, 'ROOT', base), patch.object(m, 'RAW', raw), \
                 patch.object(m, 'REPORTS', base / 'reports'), patch.object(m, 'EXTERNAL', base / 'external'):
                report = m.count_report()
            self.assertEqual(report['counts']['promotional_candidates_rss'], 2)
            self.assertEqual(report['counts']['evidence_discovery_rss'], 1)
            self.assertEqual(report['counts']['research_metadata_crossref'], 1)
            self.assertEqual(report['by_role']['promotional_content_candidate'], {'raw_rows': 3, 'unique_records': 2})
            self.assertEqual(report['by_source_type']['authorized_meta_ad_account'], {'raw_rows': 1, 'unique_records': 1})
            self.assertEqual(report['totals'], {'raw_rows': 7, 'unique_records': 6})


if __name__ == '__main__':
    unittest.main()
