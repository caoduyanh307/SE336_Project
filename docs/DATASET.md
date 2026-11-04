# Dữ liệu và gán nhãn

Nguồn chính của dự án là **bài đăng Threads tiếng Việt về thực phẩm bổ sung và sản phẩm dinh dưỡng**. Luồng cần hoàn thiện: bài đăng → vai trò nội dung → claim → sản phẩm → bằng chứng có thời điểm → kết quả kiểm chứng. Cách chạy xem [README](../README.md); số liệu và phần còn thiếu xem [STATUS](STATUS.md).

## Vai trò từng nguồn

| Nguồn | Đơn vị dữ liệu | Công dụng |
| --- | --- | --- |
| Threads API | Post ID, snapshot nội dung, quan hệ keyword | Dataset chính để tìm nội dung quảng bá và claim ứng viên |
| RSS đã cấu hình | Tiêu đề, tóm tắt, URL | Nội dung quảng bá hoặc đầu mối tài liệu phụ trợ |
| Crossref | Metadata DOI | Tìm nghiên cứu để rà mức liên quan đến claim |
| Metadata đối tác được phép dùng | Hồ sơ sản phẩm | Hỗ trợ định danh và đối chiếu thuộc tính |
| ViEcomNER hiện có | Snapshot tiêu đề sản phẩm với entity spans | Dữ liệu lịch sử giữ để đối chiếu; giữ split nguồn, không có importer hoặc NER tool trong luồng đang chạy |

Keyword hit chưa xác nhận quảng cáo trả phí, nguồn thương hiệu hoặc claim sai. RSS discovery và DOI metadata cần được rà trước khi trở thành evidence. ViEcomNER không cung cấp nhãn kiểm chứng claim. Không cộng các nguồn thành một số “quảng cáo Threads”.

Daily tập trung Threads. RSS chỉ được thu khi chủ động dùng `--with-rss`; Meta/brand/partner vẫn là nguồn tùy chọn theo cấu hình/quyền hiện có. Không tự tải thêm dataset NER.

## File và schema Threads

| Đường dẫn | Nội dung |
| --- | --- |
| `data/raw/threads_posts.jsonl` | Snapshot có `raw_text`, khi chế độ lưu toàn văn hợp lệ |
| `data/raw/threads_post_refs.jsonl` | Tham chiếu/metadata; không chứa nội dung toàn văn |
| `data/state/threads_collector.sqlite` | Dedupe, phiên bản, keyword provenance, checkpoint, quota, outbox, suppression |
| `data/events/threads_events_YYYYMMDD.jsonl` | Sự kiện từ collector |
| `data/events/new_events_YYYYMMDD.jsonl` | Sự kiện orchestration daily |
| `data/auto_baseline/threads/` | CSV claim/post ứng viên tự động; tách riêng artifact người gán nhãn |

Mỗi raw record có các nhóm trường:

- **Định danh:** `post_id`, `permalink`, `source_id=threads`, `source_platform=threads`, `source_role=threads_post_candidate`, `collection_method=official_threads_api`.
- **Thời gian/phiên bản:** `published_at`, `observed_at`, `content_hash`, `version`, `media_type`.
- **Nguồn tìm thấy:** `search_queries`; SQLite giữ quan hệ từng keyword, export hợp nhất quan hệ này.
- **Phân loại:** `post_role`, `confidence`, `needs_human_review`, `review_status`, `original_paid_ad_confirmed=false`.
- **Quyền sử dụng:** `collection_permission_status`, `storage_permission_status`, `redistribution_status`, `policy_reviewed_at`, `source_terms_url`, `license_status`, `retention_mode`.
- **Nội dung:** `raw_text` chỉ có trong snapshot được lưu toàn văn; refs/media-only không được giả định có text để trích claim.

`confidence` là điểm của rule baseline, không phải xác suất đã hiệu chỉnh hoặc độ chính xác đo được. `observed_at` là thời điểm thu, không phải thời điểm tác giả sửa bài. Timestamp có timezone; ngày trong tên spool là giờ Việt Nam.

## Chống trùng, phiên bản và event

Một bài match nhiều keyword vẫn là một `post_id`. Hash nội dung Threads dùng text và media type; thay đổi thời gian quan sát, thứ tự trang, keyword hoặc engagement không tạo phiên bản mới. RSS hash dùng tiêu đề và summary, với URL canonical làm định danh ổn định.

Hiện lưu các snapshot nội dung mới từng quan sát: A→B→A không thêm A lần nữa. “Phiên bản mới nhất” là snapshot mới gần nhất được lưu, chưa phải lịch sử edit đầy đủ hoặc bảo đảm trạng thái hiện tại của nguồn.

Event chứa `event_id`, `event_type`, `post_id`, `source_version`, `event_time`, `ingestion_time`, `source_role`, cờ replay và payload metadata. Event ID Threads ổn định theo platform/post/version/type; consumer phải dedupe bằng ID này. SQLite và JSONL không có transaction chung; outbox hỗ trợ phục hồi ít nhất một lần, không bảo đảm exactly-once toàn pipeline.

Backfill/replay có `historical_replay=true`/`is_replay=true`; không báo thành dữ liệu mới thu trong ngày. Snapshot count, số bài duy nhất, số bài quảng bá ứng viên và số claim gold phải được báo riêng. Spool local chưa phải Kafka hoặc provider push stream.

## Lưu trữ, suppression và export

Token và quyền app được kiểm riêng với chế độ lưu dữ liệu. `metadata_only` chỉ lưu tham chiếu; `full_text` lưu text trong thời hạn đã cấu hình. Các cờ local ghi nhận quyết định vận hành, không tự cấp quyền API hoặc quyền phát hành dataset. Bản refs vẫn có thể nhận diện người dùng qua permalink.

Giữ token, raw text, state DB, event dumps và báo cáo có dữ liệu cá nhân ngoài Git. Collector không thu profile hoặc suy luận hồ sơ sức khỏe cá nhân. Mặc định không phát hành toàn văn Threads. Export text chỉ hợp lệ khi đã xác định quyền tái phân phối.

Lệnh `suppress` chặn thu lại và loại nội dung tương ứng khỏi managed raw/spool/candidates/registered exports. `purge` xử lý snapshot hết hạn; export và baseline cũng lọc expiry/suppression. Các lệnh này thay đổi dữ liệu và chỉ chạy theo yêu cầu xử lý cụ thể. Bản sao downstream, backup và export không đăng ký cần được xử lý cùng quyết định xóa; không tái sử dụng bản export cũ sau suppression.

## Gán nhãn bài và claim

| Vai trò bài | Cách dùng |
| --- | --- |
| `brand_promotional_post` | Có nội dung thúc đẩy mua/dùng; người rà cần xác minh nguồn thương hiệu |
| `affiliate_promotion_candidate` | Có dấu hiệu referral/affiliate; quan hệ thương mại cần được rà |
| `user_review` | Trải nghiệm cá nhân; không tự là kết luận hiệu quả phổ quát |
| `discussion` | Trao đổi, hỏi đáp, tin tức; không đủ cơ sở xác nhận quảng bá |
| `unknown` | Thiếu ngữ cảnh hoặc chưa giải quyết được vai trò |

Baseline tạo `claim_candidate` và để trống verdict. Người gán giữ `post_id`, phiên bản, nguyên văn span, offsets 0-based Python characters, loại claim, sản phẩm, điều kiện và thời điểm. Một atomic claim chỉ có một mệnh đề có thể kiểm chứng. Ví dụ tổng hợp “X chứa vitamin C và hỗ trợ đề kháng” cần tách claim thành phần và công dụng; không đổi “hỗ trợ” thành “điều trị”. Không tự nối sản phẩm chỉ vì tên gần giống.

Mỗi evidence cần URL/DOI/document ID, tác giả/cơ quan, ngày công bố/truy cập, phiên bản, đoạn liên quan và phạm vi áp dụng. Phải khớp sản phẩm/thành phần/liều lượng/đối tượng/thời điểm của claim.

| Verdict sau rà bằng chứng | Tiêu chí |
| --- | --- |
| `SUPPORTED` | Evidence trực tiếp hỗ trợ mệnh đề với đúng điều kiện |
| `REFUTED` | Evidence trực tiếp mâu thuẫn mệnh đề trong cùng phạm vi |
| `NOT_ENOUGH_INFORMATION` | Thiếu hoặc chưa đủ evidence phù hợp để kết luận |

Không tìm thấy tài liệu không đồng nghĩa `REFUTED`. Giấy phép lưu hành hoặc nghiên cứu thành phần không xác nhận mọi công dụng của toàn bộ sản phẩm. Chỉ dùng `gold_claim` cho dữ liệu đã được người rà chấp nhận, lưu riêng khỏi output baseline.

## Đánh giá và giới hạn

Hai người gán độc lập trên pilot, so sánh role/span/product/evidence/verdict rồi adjudicate; giữ cả quyết định ban đầu và lý do xử lý bất đồng. Đo agreement trên những mẫu thực sự được gán đôi. Khi có gold, báo span F1, retrieval Recall@k và verification Macro-F1 cùng phân bố lớp/số mẫu.

Giữ các phiên bản cùng post, cùng sản phẩm và nội dung gần trùng trong cùng split; kiểm thời gian để tránh dùng evidence tương lai. ViEcomNER giữ train/validation/test nguồn; mọi sửa lỗi/trừ duplicate phải qua tập dẫn xuất có manifest, không sửa âm thầm bản gốc.

Từ khóa thiên về thực phẩm bổ sung; có thể lẫn review, tin tức và thảo luận. Search ranking, quota, page cap, visibility và deletion hạn chế recall; luôn `coverage_unknown`. Text trong ảnh/video nằm ngoài baseline hiện tại. Test synthetic chỉ xác thực code, không phải corpus thật hoặc bằng chứng chất lượng mô hình.
