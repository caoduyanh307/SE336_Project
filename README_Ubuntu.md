# SE363 — Mở rộng dữ liệu: hướng dẫn Ubuntu

Gói này chỉ **bổ sung** một script và danh mục RSS; không thay thế collector.py, rss_se363.py, claim_baseline_se363.py hoặc dữ liệu đã thu thập.

## Cài đặt

Chép `expand_se363.py` và `sources/rss_sources_se363.json` vào thư mục dự án `~/SE363_Auto_Collector`. Từ thư mục dự án với `(.venv)` đang hoạt động:

```bash
python expand_se363.py self-test
python expand_se363.py ner
python expand_se363.py rss --max-per-feed 50
python expand_se363.py report
```

Không cần cài thêm pip package. Từng lệnh có thể thực thi lại và bỏ qua bản ghi RSS đã có cùng URL.

## Dữ liệu đầu ra

- `data/external/ViEcomNER/{train,validation,test}.jsonl`: dữ liệu đã gán nhãn **NER tiêu đề sản phẩm**, *không phải quảng cáo gốc*, *không phải hồ sơ công bố sản phẩm*, *không phải bộ dữ liệu ba nhãn verification*. Không trộn tập test vào train.
- `data/raw/posts_auto.jsonl`: **ứng viên nội dung quảng bá** lấy từ RSS Store, chưa chứng minh đây là quảng cáo trả phí/chính chủ; chỉ tính là quảng cáo sau khi người rà soát xác minh nguồn và người phát biểu.
- `data/raw/evidence_discovery.jsonl`: RSS tiêu đề/tóm tắt **tìm kiếm tài liệu tham chiếu**, không phải bằng chứng đã được xác thực.
- `data/reports/ner_import_report.json`, `rss_expansion_report.json`, `dataset_roles_report.json`: thống kê số liệu thực tế, lỗi và phân loại vai trò.

### Lưu ý quan trọng về quy mô

RSS chỉ chứa các bài mới nhất và thường chỉ có 10–30 mục. `--max-per-feed 50` là **giới hạn trần**, không làm xuất hiện dữ liệu cũ. Thu thập RSS nhiều lần liên tiếp không tạo thêm nội dung mới (đã dedupe). Để có hàng trăm **quảng cáo gốc**, cần thêm nguồn từ doanh nghiệp đối tác cho phép sử dụng hoặc một dataset quảng cáo đã được cấp quyền, không dùng tin tức thay thế.

### Quyền sử dụng

- RSS website https://suckhoecong.vn/rssMain.html cho phép dùng RSS miễn phí cho cá nhân/tổ chức phi lợi nhuận, yêu cầu ghi nguồn và có thể yêu cầu ngừng sử dụng. Chỉ lưu tiêu đề + summary RSS + URL, không tự truy cập toàn văn hay tái phân phối toàn bộ bài.
- ViEcomNER https://huggingface.co/datasets/tnhyen/vietnamese-ecommerce-ner , giấy phép CC BY-NC-SA 4.0, bản được ghim `c8ba175c54b08015a40a9b4658504a02a08b7d8b`. Giữ attribution, không kinh doanh, đọc thêm quyền nội dung gốc nếu tái phân phối.
- Nếu HTTP 403/429, script ghi lỗi rồi bỏ qua; **không** đổi IP hoặc vượt CAPTCHA.

## Tiếp tục xây dựng bộ quảng cáo gốc

Lập `sources/ad_source_inventory.csv` với cột: source_name,platform,source_url,access_type,permission_basis,license_status,contact_or_owner,last_checked_at,notes. Mời 2–3 thương hiệu/đối tác cung cấp quảng cáo và metadata có giấy phép nghiên cứu; với quảng cáo Meta không có quyền API cho quảng cáo thương mại Việt Nam đại trà, sử dụng mẫu được phép ghi nhận trực tiếp và minh bạch phạm vi. Từ dữ liệu quảng cáo đã xác thực, đánh giá extraction & verification trên tập gold được annotator kiểm tra.
