# SE363 — Bộ thu thập tự động (starter)

Không dùng Selenium để vượt CAPTCHA, không lấy cookies phiên đăng nhập và không thu thập các trang Meta không có API được cấp quyền.

## Chạy ngay trên Windows + VS Code

1. Giải nén ZIP, mở thư mục `SE363_Auto_Collector` trong VS Code.
2. Trong Terminal (PowerShell):

```powershell
py -3 -m venv .venv  # không cần cài thêm thư viện Python
Copy-Item .env.example .env
.\.venv\Scripts\python.exe collector.py --sources gdelt,crossref
```

Các nguồn mặc định tạo dữ liệu **chỉ mục bài báo và metadata nghiên cứu**, **KHÔNG PHẢI** bài quảng cáo gốc, không phải bằng chứng đủ để kết luận claim. Chỉ khi API phản hồi mới có dữ liệu trong file CSV/JSONL. Không có mạng, bị chặn hoặc không có kết quả: xem `data/reports/ingestion_report.json`.

## Lấy quảng cáo Meta thật qua API được cấp quyền

Meta Ad Library API không cho tải đại trà mọi quảng cáo thực phẩm bảo vệ sức khỏe tại Việt Nam. Nếu bạn sở hữu hoặc được chủ tài khoản quảng cáo ủy quyền cho một ad account, hãy điền vào `.env`:

```ini
META_ACCESS_TOKEN=<token-cua-ban>
META_AD_ACCOUNT_ID=<account-id-cua-ban>
META_API_VERSION=v25.0
```

Token phải được cấp quyền phù hợp (thường có `ads_read`) và truy cập được tài khoản. Không đưa token vào Git hoặc ảnh chụp màn hình. Xác minh phiên bản Graph API đang được hỗ trợ.

```powershell
.\.venv\Scripts\python.exe collector.py --sources meta --claims
```

Chương trình chỉ lấy quảng cáo của ad account được cấp quyền; truy xuất `Ad Creative` để đọc văn bản khi Meta cung cấp; bỏ qua creative không có text. `source_url` trống khi API không trả về đường dẫn công khai, `ad_id` và `source_locator` là định danh truy vết. `record_status=needs_review` để kiểm tra chất lượng, không phải sai/đúng.

## Lấy bài từ RSS của thương hiệu (chỉ khi có quyền sử dụng)

Sửa `sources/approved_brand_feeds.json` từ `[]` thành danh sách **nguồn bạn có quyền thu thập**, ví dụ cấu trúc (URL dưới đây chỉ là placeholder, không phải nguồn đã kiểm chứng):

```json
[
  {
    "advertiser": "Ten thuong hieu duoc cap quyen",
    "url": "https://example.org/rss.xml",
    "authorized_for_collection": true
  }
]
```

Sau đó:

```powershell
.\.venv\Scripts\python.exe collector.py --sources brand --claims
```

RSS được phân loại `promotional_content_candidate`, **không tự gọi là quảng cáo trả phí**. Chương trình chỉ đọc feed được cấu hình, không tự crawl các trang được liên kết.

## Metadata sản phẩm do đối tác cung cấp hoặc được phép tái sử dụng

Điền các dòng thật vào `sources/partner_products.csv`, bao gồm URL tài liệu và mô tả quyền dùng dữ liệu trong cột `usage_permission`.

```powershell
.\.venv\Scripts\python.exe collector.py --sources partner
```

Nguồn Cục An toàn thực phẩm và Cổng dịch vụ công Bộ Y tế chưa được xác minh có API công khai ổn định cho các hồ sơ; chương trình không tự giả định có API, không tự cố vượt hạn chế truy cập.

## Kết quả

- `data/raw/posts_auto.csv` — quảng cáo Meta được cấp quyền và/hoặc *ứng viên* quảng bá từ RSS.
- `data/raw/claim_candidates_auto.csv` — claim regex **ứng viên**, chưa xác thực.
- `data/raw/evidence_discovery.csv` — chỉ mục URL bài báo GDELT.
- `data/raw/research_metadata.csv` — metadata DOI Crossref.
- `data/raw/product_metadata.csv` — hồ sơ sản phẩm đối tác, có version hash.
- `data/reports/ingestion_report.json` — số bản ghi nhận được/mới/lỗi.

Tất cả số lượng được tạo từ response thật hoặc file đối tác thật. Chạy lặp nhiều lần: `_key` giúp tránh trùng. File JSONL là log append-only; việc cập nhật record nên được xử lý ở bước database sau.

## Cần đánh giá thêm trước khi dùng cho đồ án

- Kiểm tra license/terms, robots với bất kỳ crawler HTML mới nào. Không tự tải toàn văn bài báo từ GDELT.
- GDELT chỉ tạo bài báo *liên quan*; không được suy ra bài báo chính là quảng cáo, hoặc bằng chứng được xác minh.
- Crossref chỉ cung cấp metadata nghiên cứu; không xem DOI là xác nhận chất lượng nghiên cứu.
- Không dùng metadata sản phẩm còn thiếu làm lý do tự động gán `REFUTED`.
- Baseline regex có thể tách sai claim. Đánh giá thủ công và lưu span chính xác trước khi dùng làm ground truth.
- Lịch định kỳ sau này: Windows Task Scheduler chạy lệnh một lần/ngày; hoặc Airflow quản lý batch cập nhật. Kafka/Spark dùng để xử lý luồng sự kiện, không gắn vào chỉ để trình diễn.
