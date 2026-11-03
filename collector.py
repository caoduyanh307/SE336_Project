"""SE363: thu thap co truy nguyen, khong vuot quyen truy cap.

Default sources: GDELT (news discovery), Crossref (research metadata).
Optional: authorized Meta ads accounts, explicitly approved brand RSS feeds,
partner-provided product metadata. No source is silently treated as verified truth.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
from pathlib import Path
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import time
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"
REPORTS = ROOT / "data" / "reports"

POSTS = RAW / "posts_auto.jsonl"
EVIDENCE = RAW / "evidence_discovery.jsonl"
RESEARCH = RAW / "research_metadata.jsonl"
PRODUCTS = RAW / "product_metadata.jsonl"
CLAIMS = RAW / "claim_candidates_auto.jsonl"

QUERY_NEWS = '("thực phẩm bảo vệ sức khỏe" OR "quảng cáo thực phẩm chức năng")'
QUERY_RESEARCH = 'dietary supplement advertising health claims evidence'

KEYWORDS = [
    "thành phần", "chứa", "hàm lượng", "hỗ trợ", "giúp", "tăng cường",
    "giảm", "phòng", "ngừa", "điều trị", "chữa", "hiệu quả", "cam kết",
    "không gây", "100%", "vitamin", "bổ sung", "cải thiện", "tác dụng",
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def digest(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.chunks = []

    def handle_data(self, data):
        self.chunks.append(data)


def as_text(value: object) -> str:
    if not value:
        return ""
    parser = PlainText()
    parser.feed(html.unescape(str(value)))
    return " ".join(" ".join(parser.chunks).split())


def load_local_env() -> None:
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class ApiResponse:
    def __init__(self, content: bytes):
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return json.loads(self.content)


def retry_after_seconds(value: str | None) -> float | None:
    """Read Retry-After as seconds or an RFC 7231 HTTP date (if provided)."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            return max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


class HttpSession:
    def get(self, url: str, params=None, headers=None, timeout=None):
        if params:
            url += ("&" if "?" in url else "?") + urlencode(params)
        req = Request(url, headers={
            "User-Agent": "SE363AcademicCollector/0.2 (research metadata)",
            **(headers or {})
        })
        host = urlparse(url).netloc
        for attempt in range(3):
            try:
                with urlopen(req, timeout=30) as response:
                    return ApiResponse(response.read())
            except HTTPError as exc:
                if exc.code == 429:
                    retry_after_raw = exc.headers.get("Retry-After") if exc.headers else None
                    retry_after = retry_after_seconds(retry_after_raw)
                    if attempt == 2 or (retry_after is not None and retry_after > 120):
                        hint = f"; Retry-After: {retry_after_raw}" if retry_after_raw else ""
                        raise RuntimeError(
                            f"HTTP 429 from {host}{hint}. API đang giới hạn truy cập; "
                            "dừng thử lại liên tục, chạy lại sau hoặc dùng nguồn khác."
                        ) from exc
                    # If no Retry-After is returned, use conservative backoff.
                    wait_seconds = retry_after if retry_after is not None else (15 * (2 ** attempt))
                    print(f"[http] {host}: HTTP 429, tạm dừng {wait_seconds:.0f}s trước khi thử lại")
                    time.sleep(wait_seconds)
                    continue
                if exc.code not in (500, 502, 503, 504) or attempt == 2:
                    raise RuntimeError(f"HTTP {exc.code} from {host}") from exc
                time.sleep(2 ** attempt)
            except (URLError, TimeoutError) as exc:
                if attempt == 2:
                    raise RuntimeError(f"Network error: {exc.reason if hasattr(exc, 'reason') else exc}") from exc
                time.sleep(2 ** attempt)
        raise RuntimeError("Network retries exhausted")


def session() -> HttpSession:
    return HttpSession()


def fetch_json(s: HttpSession, url: str, params: dict, *, token: str = "") -> dict:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    obj = s.get(url, params=params, headers=headers).json()
    if not isinstance(obj, dict):
        raise ValueError("Unexpected API JSON structure")
    if "error" in obj:
        err = obj["error"]
        raise RuntimeError(f"API error: {err.get('message', str(err))}")
    return obj


def parse_feed(content: bytes) -> list[dict]:
    root = ET.fromstring(content)
    def local_name(tag):
        return tag.rsplit('}', 1)[-1]
    def field(item, name):
        for child in item:
            if local_name(child.tag) == name:
                return (child.text or "").strip()
        return ""
    entries = []
    for item in root.iter():
        if local_name(item.tag) not in ("item", "entry"):
            continue
        link = field(item, "link")
        if not link:
            for child in item:
                if local_name(child.tag) == 'link':
                    link = child.get("href", "")
                    if link:
                        break
        entries.append({
            "link": link, "title": field(item, "title"),
            "summary": field(item, "summary") or field(item, "description") or field(item, "content"),
            "published": field(item, "pubDate") or field(item, "published") or field(item, "updated"),
        })
    return entries


def existing_keys(path: Path) -> set[str]:
    keys = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        keys.add(json.loads(line)["_key"])
                    except (KeyError, ValueError):
                        continue
    return keys


def append_unique(path: Path, records: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    seen = existing_keys(path)
    added = 0
    with path.open("a", encoding="utf-8") as f:
        for item in records:
            if not item.get("_key") or item["_key"] in seen:
                continue
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            seen.add(item["_key"])
            added += 1
    return added


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def gdelt(s: HttpSession) -> list[dict]:
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    obj = fetch_json(s, url, {
        "query": QUERY_NEWS, "mode": "artlist", "format": "json",
        "timespan": "7d", "maxrecords": min(250, int(os.getenv("GDELT_MAX_RECORDS", "50"))),
    })
    result = []
    for a in obj.get("articles", []):
        link = a.get("url", "")
        if not link or urlparse(link).scheme not in {"http", "https"}:
            continue
        result.append({
            "_key": "gdelt:" + digest(link),
            "source_role": "evidence_candidate", "source_type": "gdelt_news_index",
            "title": a.get("title", ""), "url": link,
            "publisher_domain": a.get("domain", ""),
            "seen_date": a.get("seendate", ""), "language": a.get("language", ""),
            "observed_at": now(),
            "note": "Chỉ là chỉ mục tin tức; không phải quảng cáo gốc, không chứng minh sự thật",
        })
    return result


def crossref(s: HttpSession) -> list[dict]:
    mail = os.getenv("CONTACT_EMAIL", "").strip()
    params = {"query": QUERY_RESEARCH,
              "rows": min(100, int(os.getenv("CROSSREF_ROWS", "20"))),
              "select": "DOI,title,author,publisher,published,URL,type"}
    if mail:
        params["mailto"] = mail
    obj = fetch_json(s, "https://api.crossref.org/works", params)
    result = []
    for a in obj.get("message", {}).get("items", []):
        doi = a.get("DOI", "")
        if not doi:
            continue
        ymd = a.get("published", {}).get("date-parts", [[]])
        result.append({
            "_key": "crossref:" + doi.lower(),
            "source_role": "research_metadata", "source_type": "crossref",
            "doi": doi, "title": as_text((a.get("title") or [""])[0]),
            "publisher": a.get("publisher", ""), "document_type": a.get("type", ""),
            "publication_date_parts": ymd[0] if ymd else [],
            "url": a.get("URL") or f"https://doi.org/{doi}",
            "observed_at": now(),
            "note": "DOI và metadata; chưa xác nhận full text, kết quả hay mức độ liên quan",
        })
    return result


def meta_owned(s: HttpSession) -> list[dict]:
    token = os.getenv("META_ACCESS_TOKEN", "").strip()
    account = os.getenv("META_AD_ACCOUNT_ID", "").strip().removeprefix("act_")
    if not token or not account:
        raise ValueError("Chưa khai báo META_ACCESS_TOKEN / META_AD_ACCOUNT_ID được cấp quyền")
    version = os.getenv("META_API_VERSION", "v25.0").strip()
    if not re.fullmatch(r"v\d+\.\d+", version):
        raise ValueError("META_API_VERSION không hợp lệ")
    url = f"https://graph.facebook.com/{version}/act_{account}/ads"
    params = {
        "fields": "id,name,status,created_time,updated_time,creative{id,body,title,object_story_spec}",
        "limit": 50,
    }
    result: list[dict] = []
    for _ in range(5):  # hard limit on pagination per run
        obj = fetch_json(s, url, params, token=token)
        for a in obj.get("data", []):
            cr = a.get("creative") or {}
            story = cr.get("object_story_spec") or {}
            text = cr.get("body") or ""
            for typ in ("link_data", "video_data", "photo_data", "template_data"):
                text = text or (story.get(typ) or {}).get("message", "")
            # Keep the advertisement's literal text, including line breaks and emoji.
            if not isinstance(text, str):
                text = str(text)
            if not text.strip():
                continue  # e.g. inaccessible creative, media-only ad
            ad_id = str(a.get("id", ""))
            result.append({
                "_key": f"meta_owned:{account}:{ad_id}:{digest(text)}",
                "post_id": "META-" + ad_id,
                "source_role": "advertisement_original", "source_type": "authorized_meta_ad_account",
                "platform": "Meta Marketing API", "advertiser": "authorized_ad_account",
                "ad_account_id": account, "ad_id": ad_id,
                "ad_name": a.get("name", ""), "raw_text": text,
                "source_url": "",  # API ad ID is NOT the same as a public Ad Library URL
                "source_locator": f"act_{account}/ads/{ad_id}",
                "published_at": a.get("created_time", ""), "observed_at": now(),
                "record_status": "needs_review", "content_hash_sha256": digest(text),
                "note": "Chỉ tài khoản sở hữu/được cấp quyền; kiểm tra văn bản quảng cáo và nguồn",
            })
        next_url = (obj.get("paging") or {}).get("next")
        if not next_url:
            break
        # Never send access token in a URL that may leak to logs.
        # Meta's next link normally carries the token; use cursor instead.
        after = ((obj.get("paging") or {}).get("cursors") or {}).get("after")
        if not after:
            break
        params["after"] = after
    return result


def brand_feeds(s: HttpSession) -> list[dict]:
    feeds = json.loads((ROOT / "sources" / "approved_brand_feeds.json").read_text(encoding="utf-8"))
    result = []
    for feed in feeds:
        if feed.get("authorized_for_collection") is not True:
            continue
        url = feed.get("url", "")
        if not url.startswith("https://"):
            continue
        # Respect the supplied feed only; never spider its links.
        r = s.get(url, timeout=(10, 30))
        r.raise_for_status()
        parsed = parse_feed(r.content)
        for e in parsed[:100]:
            link = e.get("link", "")
            if not link:
                continue
            content = as_text(e.get("summary", "") or e.get("description", ""))
            if not content:
                continue
            result.append({
                "_key": "brand_feed:" + digest(link + content),
                "post_id": "FEED-" + digest(link)[:12],
                "source_role": "promotional_content_candidate", "source_type": "approved_brand_rss",
                "platform": "brand_website_rss", "advertiser": feed.get("advertiser", ""),
                "raw_text": content, "title": as_text(e.get("title", "")),
                "source_url": link, "published_at": e.get("published", ""),
                "observed_at": now(), "record_status": "needs_review",
                "content_hash_sha256": digest(content),
                "note": "RSS chưa chứng minh đây là quảng cáo có trả tiền; phải phân loại sau",
            })
    return result


def partner_products() -> list[dict]:
    path = ROOT / "sources" / "partner_products.csv"
    result = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if not row.get("product_name") or not row.get("source_url") or not row.get("usage_permission"):
                continue
            # The permission field must describe an actual agreement/public reuse allowance.
            payload = {k: (v or "").strip() for k, v in row.items()}
            version = digest(json.dumps(payload, sort_keys=True, ensure_ascii=False))
            result.append({
                "_key": "partner_product:" + (row.get("product_id") or digest(row["source_url"])) + ":" + version,
                "source_role": "product_metadata", "metadata_version": version,
                "source_type": "partner_supplied", "observed_at": now(), **payload,
                "note": "Chưa phải bằng chứng khẳng định mọi hiệu quả quảng cáo",
            })
    return result


def claims_from_posts(path: Path) -> list[dict]:
    result = []
    for post in read_jsonl(path):
        raw = post.get("raw_text", "")
        post_id = post.get("post_id", "")
        if not raw or not post_id:
            continue
        spans = re.finditer(r"[^\n.!?]+(?:[.!?]+|$)", raw)
        for match in spans:
            claim_text = match.group(0).strip()
            if len(claim_text) < 12 or not any(k in claim_text.lower() for k in KEYWORDS):
                continue
            claim_id = "CLM-" + digest(post_id + claim_text)[:16]
            result.append({
                "_key": claim_id, "claim_id": claim_id,
                "post_id": post_id, "claim_text": claim_text,
                "normalized_claim": "", "product_id": "", "label": "",
                "review_status": "candidate_requires_human_review",
                "source_type": post.get("source_type", ""), "observed_at": now(),
                "note": "Rule-based candidate only; may not be atomic or verifiable",
            })
    return result


def export_csv(jsonl: Path, dest: Path) -> None:
    rows = read_jsonl(jsonl)
    if not rows:
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in row.items()})


def main() -> None:
    load_local_env()
    parser = argparse.ArgumentParser(description="SE363 lawful automated data ingestion")
    parser.add_argument("--sources", default="gdelt,crossref",
                        help="Comma separated: gdelt,crossref,meta,brand,partner")
    parser.add_argument("--claims", action="store_true", help="Tạo claim ứng viên từ posts_auto")
    args = parser.parse_args()
    options = {s.strip() for s in args.sources.split(",") if s.strip()}
    allowed = {"gdelt", "crossref", "meta", "brand", "partner"}
    if options - allowed:
        parser.error(f"Unknown sources: {sorted(options - allowed)}")
    RAW.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    s = session()
    tasks = {
        "gdelt": (lambda: gdelt(s), EVIDENCE),
        "crossref": (lambda: crossref(s), RESEARCH),
        "meta": (lambda: meta_owned(s), POSTS),
        "brand": (lambda: brand_feeds(s), POSTS),
        "partner": (partner_products, PRODUCTS),
    }
    report = {"run_at": now(), "results": {}, "errors": {}, "note": "Số thực tế, không có bản ghi giả"}
    for name in sorted(options):
        fn, target = tasks[name]
        try:
            rows = fn()
            count = append_unique(target, rows)
            report["results"][name] = {"retrieved": len(rows), "new": count, "target": str(target.relative_to(ROOT))}
            print(f"[{name}] Found: {len(rows)} | NEW saved: {count}")
        except (ValueError, RuntimeError, OSError, ET.ParseError, json.JSONDecodeError) as exc:
            report["errors"][name] = str(exc)[:500]
            print(f"[{name}] ERROR: {str(exc)[:240]}")
    if args.claims:
        count = append_unique(CLAIMS, claims_from_posts(POSTS))
        report["results"]["claim_candidates"] = {"new": count, "target": str(CLAIMS.relative_to(ROOT))}
        print(f"[claims] NEW candidates: {count}")
    for source in (POSTS, EVIDENCE, RESEARCH, PRODUCTS, CLAIMS):
        export_csv(source, source.with_suffix(".csv"))
    (REPORTS / "ingestion_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Report: data/reports/ingestion_report.json")


if __name__ == "__main__":
    main()
