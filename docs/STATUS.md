# Trạng thái dự án

Cập nhật rà soát **04/11/2026, Asia/Ho_Chi_Minh**. Hướng được người dùng xác nhận: **thu dữ liệu Threads thật, lọc nội dung quảng bá, tách claim và nối bằng chứng**. ViEcomNER và các nguồn RSS/Crossref hiện có là phụ trợ.

## Đã có và chưa có

| Phần | Trạng thái đã xác minh |
| --- | --- |
| Threads collector | Có CLI audit/daily/backfill/report/export, giới hạn request, dedupe/version, SQLite checkpoint/outbox và local tests |
| Daily | Thu Threads làm chính; RSS cần `--with-rss`; giữ legacy offsets, tổng event gồm outbox và scanner, replay có cursor riêng |
| Claim baseline | Regex tạo candidate với span nguyên văn; lọc phiên bản/expiry/suppression |
| Threads API live | Token đã được chương trình nạp sau khi chuyển đúng khóa cấu hình; truy vấn thử ngày 04/11 bị Meta từ chối **HTTP 403 / code 10**. Chưa có lượt tìm kiếm thành công |
| Corpus Threads | **0 post, 0 snapshot, 0 raw/ref row** theo dữ liệu local gần nhất |
| Product–claim–evidence linking | Chưa có đường xử lý hoàn chỉnh hoặc bộ gold đã đăng ký |
| Kafka/Spark/PostgreSQL | Chưa triển khai luồng chạy thật hoặc có benchmark; JSONL hiện là durable spool |
| Cron tự chạy | Script dự kiến 02:00 giờ hệ thống; chưa có log chứng minh một lượt tự chạy |

Lượt collector được ghi nhận gần nhất trong report là **23:05:39 ngày 03/11/2026**, `BLOCKED_PERMISSION`, fetched/new/updated đều 0. Daily gần nhất hoàn tất **23:02:09 ngày 03/11**, `PARTIAL`, thêm 0 event. Ngày 04/11 đã thử kết nối riêng với một query/limit=1, không lưu nội dung: môi trường sandbox không có mạng; kiểm tra ngoài sandbox nhận **HTTP 403 / code 10** từ Meta. Đây là lỗi quyền thực tế ở API, không còn là lỗi không nạp được token.

## Dữ liệu đang có

| Vai trò | Số lượng thực tế | Ghi chú |
| --- | ---: | --- |
| Threads | 0 bài / 0 phiên bản | Chưa có dữ liệu live |
| RSS promotion candidates | 15 bài | Nội dung Store cần người rà; chưa xác nhận quảng cáo gốc |
| RSS evidence candidates | 189 mục | Đầu mối tìm tài liệu, chưa là evidence đã kiểm |
| Crossref research metadata | 21 DOI | Chưa nối trực tiếp với từng claim/sản phẩm |
| Product metadata đối tác | 0 | CSV nguồn mới có header |
| Claim baseline trên RSS | 18 candidate trên 12/15 bài | Product/evidence/verdict/reviewer còn trống; không có gold |
| ViEcomNER | 2.997 dòng vật lý / 2.995 ID/text duy nhất | Train 2.397, validation 300, test 300; chỉ phục vụ NER phụ trợ |

225 raw records thuộc ba vai trò RSS/Crossref, không phải 225 quảng cáo Threads. ViEcomNER có 2 cặp duplicate hoàn toàn trong validation: `lazada_12485` dòng 271/275, `lazada_12802` dòng 272/274. Train dòng 1579 có một entity text rỗng và không khớp span; hai mã validator cùng chỉ entity này. Không thấy overlap ID/text chính xác giữa split; chưa kiểm near-duplicates. Bản gốc được giữ nguyên.

## Khoảng thiếu cần xử lý theo thứ tự

1. **Giải quyết grant thực tế của token/app.** Đã chuyển token từ khóa Meta legacy sang `THREADS_ACCESS_TOKEN` bằng chương trình không xuất giá trị bí mật, bổ sung cấu hình còn thiếu từ template và đặt quyền file `0600`. Token đã được nạp; Meta trả 403/code 10 với keyword search. Người dùng đã cung cấp kết quả Debugger: token có `threads_basic`, `threads_content_publish`, `threads_manage_replies`, `threads_manage_insights`, `threads_read_replies`, nhưng thiếu `threads_keyword_search`. Cần cấp lại qua OAuth với phạm vi tối thiểu `threads_basic,threads_keyword_search` và redirect HTTPS đã đăng ký; không tiếp tục tạo lại theo cùng luồng nếu nó vẫn chỉ cấp năm quyền cũ. App có quyền sẵn sàng thử nghiệm chưa chứng minh token đã được cấp scope hoặc đủ cấp truy cập. Không tự đổi các cờ xác minh chỉ để vượt local gate.
2. **Nghiệm thu dữ liệu thật.** Kiểm phản hồi API, ID/permalink/timestamp/text hoặc refs, số post duy nhất; chạy lại không nhân bản bài hoặc tạo version chỉ vì thời gian thu đổi. Chỉ bật daily sau khi lượt nhỏ hoạt động đúng.
3. **Kiểm cursor trên API thật.** Continuation đã sửa và kiểm thử offline: lưu cửa sổ thời gian/cursor sau từng trang ghi bền vững, tiếp tục sau cap/quota/crash, ưu tiên keyword đang dở; probe và backfill không ghi đè tiến độ daily. Còn cần nghiệm thu tính hợp lệ/thời hạn cursor và quota với token thật; luôn giữ `coverage_unknown`. Giữ nguyên fields/page size/search mode/type trong lúc cửa sổ còn dở.
4. **Xác minh lịch thực.** Cron log đang chưa chứng minh tự chạy. Môi trường audit bị từ chối đọc crontab; không tự đổi lịch, timezone hoặc cài thêm timer. Máy phải bật tại giờ chạy.
5. **Làm pilot trên bài Threads.** Gán role/claim/product/evidence theo [DATASET](DATASET.md), xây gold trước khi công bố accuracy/F1. Chọn NER/model theo khoảng thiếu thực tế của pilot.

Thiếu quyền API/token là `BLOCKED_PERMISSION`; chưa quyết định chế độ lưu là `BLOCKED_POLICY`; flag daily tắt là `SKIPPED_CONFIG_DISABLED`. Chưa có scope không được giải quyết bằng việc tự ghi “verified” vào `.env`. Quyền API, quyền lưu và quyền phát hành là những quyết định riêng.

## Kiểm tra đã có

| Kiểm tra sau dọn | Kết quả |
| --- | --- |
| `python -W error::ResourceWarning -m unittest discover -s tests -q` | **81 tests PASS**, fixture chỉ ở temp roots; không gọi API live |
| Continuation Threads | Qua các trường hợp cap qua ngày, quota/crash, không đổi cửa sổ, đổi request shape báo lỗi, probe/backfill độc lập |
| Daily mặc định | RSS không tự chạy; `--with-rss` vẫn hoạt động; lỗi RSS cũ không làm sai trạng thái lượt Threads |
| SHA-256 dữ liệu/state trước–sau dọn | **15/15 file giữ nguyên**, gồm raw, external, processed và state |
| CLI help, link trong ba tài liệu, `git diff --check`, bash syntax wrapper cron | PASS |
| Audit cấu hình Threads local | Exit 2 đúng với `BLOCKED_PERMISSION`, `token_present=false`, không gọi mạng |

Cây mã nguồn/cấu hình/tài liệu giảm **48 → 25 file**; tài liệu còn **README + DATASET + STATUS**. Các tests phục vụ nhánh NER đã bỏ được gỡ cùng chức năng; kiểm thử của các đường chạy giữ lại vẫn được chạy đầy đủ. Dữ liệu khảo sát NER trước dọn chỉ là dữ liệu lịch sử, không được coi là mục tiêu collector hiện tại.

Các regression quan trọng đã có: multi-page/cursor/quota và API errors; unchanged/content edit; outbox crash/recovery; không đếm lại event đã có/suppressed/purged; legacy retry qua nửa đêm; replay BUSY/crash không reset normal cursor; exact claim spans/decimals; RSS content versions và provenance counts.

## Quyết định dọn cấu trúc

| Nhóm cũ | Chỗ duy trì thống nhất |
| --- | --- |
| Nhiều README cài ZIP/Ubuntu và hướng dẫn chạy Threads | [README](../README.md) |
| Dataset card, annotation và phần schema/retention | [DATASET](DATASET.md) |
| Baseline audit, acceptance, checklist, streaming status, project review | Tài liệu trạng thái này |
| Hai script/config RSS trùng chức năng | Một đường RSS chính; giữ permission provenance của config đã gom |
| Hai template môi trường | Một `.env.example`, Threads đứng đầu |
| Công cụ validator/importer ViEcomNER và các mode `ner`/`all` | Bỏ code không phục vụ thu Threads; giữ snapshot dữ liệu lịch sử |
| Prompt/PDF bàn giao sao chép, implementation patch và timer mẫu chưa dùng | Bỏ khỏi cây làm việc sau backup; giữ yêu cầu cần thiết trong ba tài liệu trên |

Backup trước dọn: `/tmp/SE363_before_cleanup_20261104_135219.tar.gz`. Dữ liệu nguồn, annotation, `.env`, môi trường Python, SQLite, normal/replay offsets và lịch cron hiện hữu được giữ. Không dùng việc dọn file để reset dữ liệu hoặc bỏ kiểm thử chức năng.

## Căn cứ kỹ thuật và giới hạn nghiệm thu

Đã đối chiếu tài liệu Meta công khai: [request keyword search](https://www.postman.com/meta/threads/request/34203612-b3b2c12a-7ce6-4d86-a3c6-6d31e3b66ea1), [collection của Meta](https://raw.githubusercontent.com/fbsamples/threads_api/main/postman/threads-api.postman_collection.json), [sample API](https://github.com/fbsamples/threads_api). Các nguồn mô tả endpoint/params/OAuth; authenticated cursor behavior, quota và quyền cụ thể của app vẫn cần probe thật. Cấu hình chấp nhận hai API origin chính thức `.com`/`.net`, không gửi bearer qua redirect.

Nhiều trang developers/terms trả 429 hoặc yêu cầu login trong lần nghiên cứu 03/11. Đây là hạn chế truy cập tài liệu của môi trường, không chứng minh API của người dùng bị lỗi hoặc app đã có quyền. Rate cap trong code là giới hạn local, không phải quota do Meta cấp.

Kiểm tra môi trường streaming 03/11: Docker daemon bị từ chối truy cập; Kafka/PySpark/Postgres Python modules chưa có. Không cài dịch vụ hoặc báo latency/throughput giả. LIVE DONE chỉ khi có grant hợp lệ, truy vấn thật thành công và dữ liệu lưu đúng chế độ; hoàn tất toàn đồ án còn cần gold/evaluation và pipeline streaming được đo thật.
