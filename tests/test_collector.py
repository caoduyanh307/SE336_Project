import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collector import append_unique, claims_from_posts, read_jsonl, digest

class CollectorTests(unittest.TestCase):
    def test_dedup_and_claim_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'posts.jsonl'
            sample = {
                '_key':'example1', 'post_id':'TEST001',
                'raw_text':'Sản phẩm X chứa vitamin C. Sản phẩm X hỗ trợ sức khỏe.',
                'source_type':'test_only'
            }
            self.assertEqual(append_unique(path, [sample, sample]), 1)
            self.assertEqual(append_unique(path, [sample]), 0)
            self.assertEqual(len(read_jsonl(path)), 1)
            claims = claims_from_posts(path)
            self.assertEqual(len(claims), 2)
            self.assertEqual({c['label'] for c in claims}, {''})
            self.assertTrue(all('requires_human_review' in c['review_status'] for c in claims))
    def test_digest(self):
        self.assertEqual(digest('abc'), digest('abc'))

if __name__ == '__main__':
    unittest.main()

class LocalApiContractTests(unittest.TestCase):
    def test_feed_parser(self):
        from collector import parse_feed
        feed = b'<rss><channel><item><title>Product update</title><link>https://example.org/x</link><description>Product feature</description></item></channel></rss>'
        entries = parse_feed(feed)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]['link'], 'https://example.org/x')

    def test_gdelt_is_evidence_candidate_not_ad(self):
        from collector import gdelt
        class FakeSession:
            def get(self, url, params=None, headers=None, timeout=None):
                class Response:
                    def json(self):
                        return {'articles':[{'url':'https://example.org/news','title':'News','seendate':'20261102'}]}
                return Response()
        data = gdelt(FakeSession())
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['source_role'], 'evidence_candidate')
        self.assertNotEqual(data[0]['source_role'], 'advertisement_original')
