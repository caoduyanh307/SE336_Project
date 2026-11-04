# SE363 — Thu thập Threads và kiểm chứng claim sản phẩm

Nguồn chính: **bài Threads tiếng Việt về thực phẩm bổ sung và sản phẩm dinh dưỡng**. Luồng đang xây là thu bài → phân loại nội dung → trích claim → nối sản phẩm/bằng chứng → gán nhãn và đánh giá. Collector, lưu phiên bản, event spool và claim baseline đã có; liên kết evidence và benchmark gold còn đang làm.

## Bắt đầu với token vừa tạo

Dự án dùng **Python 3.10+ và thư viện chuẩn**, không cần cài thêm package. Dùng `.venv/bin/python` hiện có; lần đầu trên máy mới có thể tạo bằng `python3 -m venv .venv`.

1. Mở `.env` trong thư mục dự án, thêm hoặc sửa đúng một dòng `THREADS_ACCESS_TOKEN=...`. Giữ token riêng; không đưa vào chat, Git, ảnh chụp hoặc lệnh shell.
2. Dùng **một** file `.env.example` để tham khảo các khóa còn lại. Giữ `.env` hiện có; chỉ chép các khóa cần bổ sung.
3. Kiểm tra cấu hình local:

```bash
.venv/bin/python -m ingestion.threads_collector audit
```

`token_present=true` chỉ xác nhận đã đọc được token. App có quyền “Sẵn sàng thử nghiệm” chưa chứng minh token đã được grant quyền; collector cần `threads_basic` và `threads_keyword_search` thực sự được cấp.

Nếu Debugger chỉ hiện năm quyền basic/publish/manage replies/insights/read replies, token vẫn thiếu `threads_keyword_search`. Luồng OAuth phải yêu cầu `scope=threads_basic,threads_keyword_search` với đúng Threads app ID và redirect HTTPS đã đăng ký; chỉ đổi giá trị khai báo trong `.env` không làm token có thêm quyền. Xem [mẫu OAuth chính thức của Meta](https://github.com/fbsamples/threads_api/blob/main/src/index.js). Không giả định nút tạo token mặc định sẽ cấp thêm scope.

Các khóa ghi nhận cấu hình đã xác minh:

```dotenv
THREADS_COLLECTION_PERMISSION_STATUS=verified
THREADS_VERIFIED_SCOPES=threads_basic,threads_keyword_search
THREADS_STORAGE_PERMISSION_STATUS=metadata_only
THREADS_POLICY_REVIEWED_AT=YYYY-MM-DD
THREADS_REDISTRIBUTION_STATUS=not_permitted
SE363_ENABLE_THREADS_DAILY=0
```

Chỉ điền `verified`/ngày review sau khi kiểm quyền và mục đích sử dụng thật. Chọn `metadata_only` để lưu tham chiếu; `full_text` chỉ khi quyền lưu nội dung đã đáp ứng. Claim extraction cần text, nên refs-only chưa tạo được bộ claim. Các cờ local không tự cấp quyền Meta. Giới hạn lưu và cấu hình truy vấn nằm trong `.env.example`.

## Thử API rồi chạy daily

Sau khi local audit báo `READY`:

```bash
# Một truy vấn nhỏ, limit=1, có thể lưu một record/event theo chế độ đã chọn.
.venv/bin/python -m ingestion.threads_collector audit --verify-live
.venv/bin/python -m ingestion.threads_collector report
```

Probe thành công mới xác nhận endpoint hoạt động với token. Sau đó đặt `SE363_ENABLE_THREADS_DAILY=1` trong `.env` và chạy:

```bash
bash run_daily.sh
.venv/bin/python daily_se363.py --status
.venv/bin/python -m ingestion.threads_collector report
```

`run_daily.sh` dùng venv và khóa tránh chạy chồng. Daily mặc định tập trung Threads, tạo claim candidates khi có text được phép lưu, rồi cập nhật spool/report. RSS chỉ chạy khi thêm `--with-rss`:

```bash
.venv/bin/python daily_se363.py --once --with-rss
```

Không tính RSS, Crossref hoặc ViEcomNER thành quảng cáo Threads. Nhãn tự động chỉ là candidate cần người rà. Số liệu thật và giới hạn nghiệm thu xem [trạng thái](docs/STATUS.md); schema, quyền dữ liệu và quy trình gán nhãn xem [dataset](docs/DATASET.md).

## Cấu trúc duy trì

```text
README.md                         Cách cài, token và chạy
.env.example                      Một mẫu cấu hình, Threads đứng đầu
config/threads_keywords.json      Từ khóa và ghi chú sampling
ingestion/                       Threads collector, phân loại vai trò
daily_se363.py                   Điều phối, spool và trạng thái
claim_baseline_se363.py          Trích claim ứng viên từ dữ liệu local
run_daily.sh                     Entry point daily có lock
install_daily_cron.sh            Cài lịch khi chủ động yêu cầu
remove_daily_cron.sh             Gỡ lịch khi chủ động yêu cầu
collector.py                     API metadata/evidence/nguồn legacy tùy chọn
expand_se363.py                  Một công cụ RSS bổ trợ và báo cáo số liệu
sources/                         Nguồn bổ trợ và quyền của từng nguồn
tests/                           Kiểm thử offline, dùng dữ liệu tổng hợp trong temp
docs/DATASET.md                   Schema, provenance, annotation
docs/STATUS.md                    Kết quả thật, blockers và việc còn thiếu
data/                            Dữ liệu/state/output local, ngoài Git
```

Dữ liệu Threads nằm ở `data/raw/threads_posts.jsonl` hoặc `threads_post_refs.jsonl`; SQLite ở `data/state/threads_collector.sqlite`; claim tự động ở `data/auto_baseline/threads/`. Event spool và báo cáo lần lượt ở `data/events/`, `data/reports/`. Giữ annotation thủ công tách khỏi output baseline. Các dữ liệu RSS/NER đã có vẫn giữ nguyên để đối chiếu.

## Lịch 02:00

Lịch dự kiến dùng giờ hệ thống Asia/Ho_Chi_Minh; máy phải bật lúc chạy. Đợt dọn code không cài hoặc thay đổi cron. Kiểm tra bằng:

```bash
crontab -l
tail -n 50 data/logs/cron.log
```

Nếu cần cài/gỡ lịch, dùng hai script tương ứng sau khi xác nhận entry hiện tại, tránh cài trùng. Tắt riêng Threads bằng `SE363_ENABLE_THREADS_DAILY=0`, giữ nguyên state và dữ liệu.

## Kiểm thử và các lệnh bổ sung

```bash
.venv/bin/python -W error::ResourceWarning -m unittest discover -s tests -q
.venv/bin/python -m ingestion.threads_collector --help
.venv/bin/python daily_se363.py --help
.venv/bin/python expand_se363.py report
```

Collector có backfill, export, suppress và purge; xem `--help` cùng [dataset](docs/DATASET.md) trước khi dùng. Backfill/replay là dữ liệu lịch sử, không được báo như mới cào hôm nay. `--bootstrap-existing` dùng cursor replay riêng. Suppress/purge thay đổi dữ liệu và chỉ chạy theo yêu cầu cụ thể.

Phân trang lưu cửa sổ/cursor đang dở để tiếp tục sau page cap, quota hoặc gián đoạn. Trong khi một cửa sổ còn dở, giữ nguyên fields, page size và search mode/type; thay đổi sẽ báo lỗi rõ ràng. Search vẫn không bảo đảm lấy đủ mọi bài. `401/403` dừng; `429` hoãn theo retry-after; lỗi mạng/5xx retry hữu hạn. Spool local hiện chưa phải Kafka/Spark.
