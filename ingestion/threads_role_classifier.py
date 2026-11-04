"""Conservative Vietnamese post-role rules; outputs are candidates for human review.

Keyword matches do not establish paid advertising, brand ownership, fraud or truth.
No account attributes or sensitive personal information are inferred.
"""
from __future__ import annotations

import re
import unicodedata


def classify_post(text: str, search_queries=(), noise_terms=()) -> dict:
    text = unicodedata.normalize('NFC', text or '').casefold()
    patterns = {
        'affiliate': r'\b(?:affiliate|tiếp thị liên kết|link (?:ở |trong )?bio|mã (?:của mình|giới thiệu)|hoa hồng)\b',
        'commercial': r'\b(?:đặt hàng|mua ngay|inbox|giảm giá|ưu đãi|freeship|chốt đơn|đặt mua|khuyến mãi|liên hệ mua)\b',
        'first_party_sales': r'\b(?:shop mình|bên mình|cửa hàng|chúng tôi|thương hiệu|nhà phân phối)\b',
        'experience': r'\b(?:mình (?:đã |đang )?(?:dùng|uống|thử)|tôi (?:đã |đang )?(?:dùng|uống|thử)|trải nghiệm|review|đánh giá)\b',
        'discussion': r'\b(?:mọi người|ai (?:đã |đang )?(?:dùng|uống)|có nên|thảo luận|nghiên cứu|có (?:tốt|hiệu quả) không)\b',
    }
    matched = [name for name, pattern in patterns.items() if re.search(pattern, text)]
    noise = [term for term in noise_terms if unicodedata.normalize('NFC', term).casefold() in text]
    if noise:
        role, confidence = 'unknown', 0.10
    elif 'affiliate' in matched:
        role, confidence = 'affiliate_promotion_candidate', 0.65
    elif 'commercial' in matched and 'first_party_sales' in matched:
        role, confidence = 'brand_promotional_post', 0.60
    elif 'experience' in matched and 'commercial' not in matched:
        role, confidence = 'user_review', 0.60
    elif 'discussion' in matched and 'commercial' not in matched:
        role, confidence = 'discussion', 0.55
    else:
        role, confidence = 'unknown', 0.25
    return {
        'post_role': role,
        'confidence': confidence,
        'needs_human_review': True,
        'classifier_version': 'vn-role-rules-v1',
        'provenance': {'matched_rules': matched, 'matched_noise_terms': noise, 'search_queries': list(search_queries)},
        'classification_note': 'Rule candidate only; brand ownership and paid-ad status are unverified.',
    }
