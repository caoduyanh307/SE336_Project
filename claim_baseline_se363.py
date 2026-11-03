#!/usr/bin/env python3
"""SE363 D01: baseline claim CANDIDATES from RSS promotional snippets.

Consumes ONLY local existing data/raw/posts_auto.jsonl. This DOES NOT
establish original advertisement provenance, validate truth, or find evidence.
It never makes network requests and never edits source files.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile
import unicodedata

VN_TZ = timezone(timedelta(hours=7))
SEGMENTS = re.compile(r'[^.!?;\n]+(?:[.!?;]+|(?=\n)|$)', re.UNICODE)
RULES = [
    ("thanh_phan", re.compile(r'\b(?:chứa|có chứa|bổ sung|thành phần|chiết xuất|vitamin|collagen|canxi|kẽm|omega[- ]?3|probiotic|sắt|protein)\b', re.IGNORECASE)),
    ("cong_dung", re.compile(r'\b(?:hỗ trợ|giúp|cải thiện|tăng cường|giảm|phòng ngừa|ngăn ngừa|điều trị|chữa|ngăn chặn|bảo vệ|phục hồi|duy trì|ổn định)\b', re.IGNORECASE)),
    ("dinh_luong", re.compile(r'\b\d+(?:[.,]\d+)?\s*(?:mg|mcg|µg|g|kg|ml|l|iu|%|viên|gói)\b', re.IGNORECASE)),
]
COMPOSITE = re.compile(r'\b(?:và|đồng thời|cùng lúc|cũng như)\b', re.IGNORECASE)


def sha(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()


def stamp() -> str:
    return datetime.now(VN_TZ).isoformat(timespec='seconds')


def normalize(text: str) -> str:
    return ' '.join(unicodedata.normalize('NFC', text).split())


def iter_phrases(text: str):
    """Original-text substrings with exact Python character offsets."""
    for match in SEGMENTS.finditer(text):
        raw = match.group()
        trimmed = raw.strip()
        if not trimmed:
            continue
        leading = len(raw) - len(raw.lstrip())
        start = match.start() + leading
        end = start + len(trimmed)
        assert text[start:end] == trimmed
        yield start, end, trimmed


POST_COLS = [
    'post_id', 'title', 'raw_text', 'source_url', 'publisher',
    'source_role', 'source_type', 'advertiser', 'observed_at',
    'original_ad_confirmed', 'product_name_confirmed',
    'review_status', 'reviewer', 'review_notes',
]
CLAIM_COLS = [
    'claim_id', 'post_id', 'claim_text', 'normalized_claim', 'claim_category',
    'span_start', 'span_end', 'needs_atomic_split', 'source_url',
    'source_role', 'source_text_field', 'product_id', 'metadata_id',
    'evidence_id', 'verification_label', 'annotation_status', 'reviewer',
    'review_notes', 'extractor_version', 'extracted_at',
]


def save_csv(path: Path, columns: list[str], records: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)


def process(source: Path, output: Path, reports: Path) -> dict:
    if not source.is_file():
        raise FileNotFoundError(f'Không tìm thấy dữ liệu nguồn: {source}')

    posts = []
    claims = []
    invalid_json_lines = []
    skipped_wrong_role = 0
    missing_summary = 0
    seen_post_keys = set()
    seen_claims = set()

    with source.open('r', encoding='utf-8-sig') as stream:
        for lineno, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_json_lines.append(lineno)
                continue
            if not isinstance(record, dict):
                invalid_json_lines.append(lineno)
                continue

            # We process only store RSS promo candidates, not evidence/news.
            if record.get('source_role') != 'promotional_content_candidate':
                skipped_wrong_role += 1
                continue
            url = str(record.get('source_url') or record.get('url') or '')
            post_id = str(record.get('post_id') or 'RSS-' + sha(url)[:12])
            key = str(record.get('_key') or post_id)
            if key in seen_post_keys:
                continue
            seen_post_keys.add(key)
            text = str(record.get('raw_text') or '')
            if not text.strip():
                missing_summary += 1
            posts.append({
                'post_id': post_id,
                'title': str(record.get('title') or ''),
                'raw_text': text,
                'source_url': url,
                'publisher': str(record.get('publisher') or ''),
                'source_role': str(record.get('source_role') or ''),
                'source_type': str(record.get('source_type') or ''),
                'advertiser': str(record.get('advertiser') or ''),
                'observed_at': str(record.get('observed_at') or ''),
                'original_ad_confirmed': '',
                'product_name_confirmed': '',
                'review_status': 'NEEDS_REVIEW',
                'reviewer': '',
                'review_notes': 'RSS Store của nhà xuất bản; KHÔNG mặc định là quảng cáo gốc',
            })

            for start, end, phrase in iter_phrases(text):
                hits = [name for name, pattern in RULES if pattern.search(phrase)]
                if not hits:
                    continue
                signature = (post_id, start, end, phrase)
                if signature in seen_claims:
                    continue
                seen_claims.add(signature)
                claims.append({
                    'claim_id': 'C-' + sha(f'{post_id}:{start}:{end}:{phrase}')[:16],
                    'post_id': post_id,
                    'claim_text': phrase,
                    'normalized_claim': normalize(phrase),
                    'claim_category': '|'.join(hits),
                    'span_start': start,
                    'span_end': end,
                    'needs_atomic_split': 'YES' if COMPOSITE.search(phrase) else 'NO',
                    'source_url': url,
                    'source_role': 'promotional_content_candidate',
                    'source_text_field': 'raw_text (RSS summary)',
                    'product_id': '',
                    'metadata_id': '',
                    'evidence_id': '',
                    'verification_label': '',
                    'annotation_status': 'CANDIDATE_NEEDS_REVIEW',
                    'reviewer': '',
                    'review_notes': '',
                    'extractor_version': 'regex-v0.1',
                    'extracted_at': stamp(),
                })

    post_path = output / 'posts_review.csv'
    claims_path = output / 'claim_candidates_review.csv'
    save_csv(post_path, POST_COLS, posts)
    save_csv(claims_path, CLAIM_COLS, claims)
    report = {
        'generated_at': stamp(),
        'input': str(source),
        'posts_for_manual_review': len(posts),
        'claims_for_manual_review': len(claims),
        'posts_containing_regex_candidates': len({c['post_id'] for c in claims}),
        'posts_without_regex_candidates': len(posts) - len({c['post_id'] for c in claims}),
        'empty_rss_summary_records': missing_summary,
        'invalid_json_lines': invalid_json_lines,
        'skipped_non_promo_roles': skipped_wrong_role,
        'outputs': [str(post_path), str(claims_path)],
        'limitations': [
            'RSS Store snippets are not verified original ads or brand-owned claims',
            'Regex only generates possible claims; verify the exact speaker, product and context',
            'Neither evidence validity nor supported/refuted/insufficient labels are established',
            'One sentence can contain several atomic claims; split during manual review',
            'Offsets are 0-based Python character positions in the original raw_text',
        ],
    }
    reports.mkdir(parents=True, exist_ok=True)
    (reports / 'claim_baseline_report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def self_test():
    with tempfile.TemporaryDirectory(prefix='se363-claim-test-') as directory:
        base = Path(directory)
        src = base / 'raw.jsonl'
        sample_text = 'Sản phẩm mẫu chứa vitamin C. Hỗ trợ miễn dịch và giúp ngủ ngon.'
        src.write_text(json.dumps({
            '_key': 'test', 'post_id': 'RSS-TEST', 'source_role': 'promotional_content_candidate',
            'source_url': 'https://example.org/item', 'raw_text': sample_text
        }, ensure_ascii=False) + '\n', encoding='utf-8')
        result = process(src, base / 'processed', base / 'reports')
        assert result['posts_for_manual_review'] == 1
        assert result['claims_for_manual_review'] == 2, result
        with (base / 'processed' / 'claim_candidates_review.csv').open(encoding='utf-8-sig', newline='') as file:
            for row in csv.DictReader(file):
                a, b = int(row['span_start']), int(row['span_end'])
                assert sample_text[a:b] == row['claim_text']
                assert row['verification_label'] == ''
        print('Self-test PASS (dữ liệu giả chỉ dùng để kiểm thử, không lưu vào data/)')


def main():
    parser = argparse.ArgumentParser(description='SE363: claim candidates from existing RSS summaries')
    parser.add_argument('--input', default='data/raw/posts_auto.jsonl')
    parser.add_argument('--out', default='data/processed')
    parser.add_argument('--reports', default='data/reports')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    result = process(Path(args.input), Path(args.out), Path(args.reports))
    print('[CLAIM BASELINE] posts_review =', result['posts_for_manual_review'])
    print('[CLAIM BASELINE] claim_candidates =', result['claims_for_manual_review'])
    print('[CLAIM BASELINE] result =', str(Path(args.out) / 'claim_candidates_review.csv'))
    print('[CLAIM BASELINE] report =', str(Path(args.reports) / 'claim_baseline_report.json'))
    print('Lưu ý: các bản ghi chưa được xác nhận là quảng cáo gốc; claim chỉ là ứng viên.')


if __name__ == '__main__':
    main()
