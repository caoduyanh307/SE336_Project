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

    def test_ner_pin_and_partition(self):
        data = (json.dumps({'id':'demo','text':'Sản phẩm giả định','entities':[]}, ensure_ascii=False) + '\n').encode('utf-8')
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            with patch.object(m, 'ROOT', base), patch.object(m, 'EXTERNAL', base/'external'), patch.object(m, 'REPORTS', base/'reports'), \
                 patch.object(m, 'request_bytes', return_value=data):
                report = m.download_ner()
                self.assertEqual([item['rows'] for item in report['files']], [1,1,1])
                self.assertTrue((base/'external'/'PROVENANCE.json').exists())


if __name__ == '__main__':
    unittest.main()
