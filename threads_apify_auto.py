#!/usr/bin/env python3
"""Collect public Threads keyword-search results using the Apify actor API.

Setup: pip install apify-client
Run: python3 threads_apify_auto.py
Token is read from APIFY_TOKEN or requested interactively (never hardcoded).
Each run overwrites the two output files. Check provider/platform terms before use.
"""
import csv
import getpass
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from apify_client import ApifyClient

ACTOR = "automation_craft/threads-scraper"
QUERIES = ["shopee", "mỹ phẩm", "giảm giá"]
OUTPUT_DIR = Path("threads_dataset")
FIELDS = [
    "post_id", "post_url", "post_text", "author_username", "posted_at",
    "keyword", "like_count", "reply_count", "repost_count", "quote_count",
    "is_reply", "collected_at", "collection_method"
]


def main():
    token = os.environ.get("APIFY_TOKEN") or getpass.getpass("Nhập Apify API token (không hiển thị): ").strip()
    if not token:
        raise SystemExit("Thiếu Apify API token.")
    client = ApifyClient(token)
    print("Đang chạy Apify Actor với giới hạn tối đa 30 bài...")
    actor_input = {
        "searchQueries": QUERIES,
        "searchMode": "recent",
        "maxPostsPerQuery": 10,
        "maxPosts": 30,
        "excludeReplies": True,
        "includeProfile": False,
        "proxyConfiguration": {"useApifyProxy": True},
    }
    try:
        run = client.actor(ACTOR).call(run_input=actor_input)
        if not run or run.get("status") != "SUCCEEDED":
            raise RuntimeError(f"Actor chưa hoàn tất/thất bại: {run.get('status') if run else 'không có phản hồi'}")
        raw_items = client.dataset(run["defaultDatasetId"]).list_items().items
    except Exception as exc:
        raise SystemExit(f"Không lấy được dữ liệu: {exc}") from exc

    OUTPUT_DIR.mkdir(exist_ok=True)
    with (OUTPUT_DIR / "raw_actor_output.json").open("w", encoding="utf-8") as f:
        json.dump(raw_items, f, ensure_ascii=False, indent=2)

    collected_at = datetime.now(timezone.utc).isoformat()
    seen = set()
    records = []
    for item in raw_items:
        if item.get("type") != "post":
            continue  # Exclude coverage, status, profile, and summary rows.
        post_id = str(item.get("postId") or "")
        post_url = item.get("url") or ""
        dedup_key = post_id or post_url
        if not dedup_key or dedup_key in seen:
            continue
        seen.add(dedup_key)
        records.append({
            "post_id": post_id,
            "post_url": post_url,
            "post_text": item.get("text") or "",
            "author_username": item.get("authorUsername") or "",
            "posted_at": item.get("postedAt") or "",
            "keyword": item.get("sourceQuery") or "",
            "like_count": item.get("likeCount"),
            "reply_count": item.get("replyCount"),
            "repost_count": item.get("repostCount"),
            "quote_count": item.get("quoteCount"),
            "is_reply": item.get("isReply"),
            "collected_at": collected_at,
            "collection_method": "apify_automation_craft_threads_scraper",
        })

    with (OUTPUT_DIR / "posts.json").open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    with (OUTPUT_DIR / "posts.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)
    print(f"Hoàn tất: {len(records)} bài duy nhất. Dataset ID: {run['defaultDatasetId']}")
    print(f"Files: {OUTPUT_DIR / 'posts.json'}, {OUTPUT_DIR / 'posts.csv'}")
    if not records:
        print("Không có bài: kiểm tra raw_actor_output.json, đặc biệt các dòng coverage/status.")


if __name__ == "__main__":
    main()
