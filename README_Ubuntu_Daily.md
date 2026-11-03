# SE363 – Collector tự động mỗi ngày trên Ubuntu

## 0. Cấu trúc, vai trò nguồn

Bộ này là **scheduler + pipeline orchestration** dựa trên các file của dự án hiện tại (`expand_se363.py`, `collector.py`, `claim_baseline_se363.py`). **Nó không cung cấp nguồn quảng cáo Facebook đại trà mới.**

- `RSS` ở `sources/rss_sources_se363.json`: thu thập tiêu đề/tóm tắt tài liệu tham chiếu; mục Store chỉ là *ứng viên nội dung quảng bá*.
- `brand` tại `sources/approved_brand_feeds.json`: chỉ chạy nếu nguồn được bật `authorized_for_collection: true` dựa trên **quyền thực tế** (không bật chỉ để vượt điều kiện).
- `Meta` quảng cáo gốc: chỉ dùng tài khoản bạn sở hữu hoặc được cấp quyền, `SE363_ENABLE_META_DAILY=1` cộng với `META_ACCESS_TOKEN`, `META_AD_ACCOUNT_ID` trong `.env`.
- `GDELT` không được tự chạy mỗi ngày vì bạn đã gặp 429; Crossref không cần query lại hàng ngày. Dataset NER chỉ nhập một lần.
- Chạy `claim_baseline_se363.py` ra **`data/auto_baseline`** để không ghi đè dữ liệu đã annotation thủ công.

## 1. Cài nhanh

Giải nén các file của ZIP vào **ngay thư mục** `~/SE363_Auto_Collector` (cùng cấp với `collector.py`), không giải nén thành một thư mục con.

```bash
cd ~/SE363_Auto_Collector
unzip -o ~/Downloads/SE363_Daily_Automation.zip
source .venv/bin/activate
python daily_se363.py --self-test
python daily_se363.py --once
python daily_se363.py --status
```

Mọi lần chạy ghi `data/reports/daily_latest.json`, `data/reports/daily_history.jsonl`, `data/logs/daily_*.log`; file `data/events/new_events_YYYYMMDD.jsonl` chứa bản ghi mới nối thêm từ lần chạy trước, phục vụ **đầu vào dự kiến cho Kafka producer** (chưa phải Kafka đang chạy).

Lần đầu, `daily_se363.py` ghi nhớ offset hiện tại rồi mới thu thập, vì vậy **188 RSS cũ và 15 ứng viên quảng bá cũ không được đánh dấu thành dữ liệu thu thập mới trong ngày**. Dataset gốc vẫn được giữ nguyên.

### Tự động 02:00 hằng ngày

```bash
sudo apt update && sudo apt install -y cron
sudo systemctl enable --now cron
bash install_daily_cron.sh
crontab -l
```

**Cron dùng múi giờ hệ thống**, kiểm tra `date` hoặc `timedatectl`, bảo đảm thời gian mong đợi là giờ Việt Nam. Máy Ubuntu cần được bật tại thời điểm chạy; nếu ngủ/tắt sẽ bỏ lỡ lượt chạy. Muốn chạy kể cả khi laptop tắt cần triển khai lên máy chủ/VPS hoặc Airflow scheduler trên máy chủ hoạt động liên tục.

**Thử thủ công ngay:**

```bash
bash run_daily.sh
cat data/reports/daily_latest.json
ls -lh data/events/
tail -n 60 data/logs/cron.log
```

*Lưu ý:* `cron.log` chỉ có sau khi cron thực sự chạy; còn kiểm tra thử bằng `run_daily.sh` thì xem file `daily_*.log`.

Hủy lịch:

```bash
bash remove_daily_cron.sh
```

## 2. Muốn tăng quảng cáo gốc, không chỉ tăng bài báo

Hiện 14 RSS khai thác chủ yếu từ **một nhà xuất bản**, nên nhiều lần chạy có thể thêm 0 bài. Chạy hàng ngày không tự tạo dữ liệu mới. Để tăng quảng cáo gốc:

1. Xin quyền tài khoản Meta Marketing API từ đối tác (chỉ quảng cáo sở hữu/được cấp quyền); hoặc
2. Ký thỏa thuận với thương hiệu cung cấp RSS/API nội dung quảng bá; cấu hình `approved_brand_feeds.json` với nguồn và điều kiện cho phép; hoặc
3. Xin phép sử dụng bộ VietAestheticAds (VietCheckMed AAAI 2026) từ nhóm tác giả nếu bộ dữ liệu được cung cấp; không giả định sẵn tải được.

**Không sử dụng đổi proxy, vượt CAPTCHA, quét Facebook trái điều khoản.** Nên ghi nhận count quảng cáo hợp lệ riêng với bài tin tức và tiêu đề NER.

## 3. Đồ án Big Data thực chất

1. **AI chất lượng**: tập quảng cáo gốc xác minh + annotation claim + metadata/bằng chứng + nhãn ba trạng thái, đo macro-F1 và evidence retrieval.
2. **Streaming**: gửi *các event mới* từ `data/events/` vào Kafka, Spark Structured Streaming đọc và xử lý micro-batch, database lưu kết quả.
3. **Scalability**: sử dụng *streaming replay* dữ liệu đã thu thập hợp pháp với nhiều tốc độ giả lập, ghi `throughput`, `latency p50/p95`, consumer lag, fault recovery. Phải gọi là replay, không được nói đó là live Facebook crawler.
4. **Temporal**: định kỳ refresh metadata sản phẩm/nguồn, version hóa và tái kiểm chứng claim liên quan.

## 4. Giải thích các kết quả

- `events_new=0`: không phát hiện bản ghi nguồn mới kể từ offset lần trước; **không phải lỗi**.
- `status=PARTIAL`: RSS/Meta/brand có nguồn trả lỗi hoặc quota; xem report và log.
- `status=FAILED`: script/stage bị lỗi; xem `failed_stages` và log.
- Mọi candidate từ RSS Store vẫn phải rà soát trước khi xác nhận quảng cáo gốc.

## 5. Nguồn chính thức

- RSS và giới hạn dùng: https://suckhoecong.vn/rssMain.html
- Apache Airflow cron: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/cron.html
- VietCheckMed AAAI 2026: https://ojs.aaai.org/index.php/AAAI/article/view/37077

## 6. Tạo luồng replay cho dữ liệu đã có (tùy chọn)

Nếu muốn đưa **15 bài Store + 188 URL evidence cũ** vào luồng Kafka để thử nghiệm, chạy **một lần**:

```bash
python daily_se363.py --bootstrap-existing
```

Sự kiện này có `historical_replay: true`, không được báo cáo là bản ghi mới thu trong ngày. Đây là event JSONL **để đưa vào Kafka** chứ không tự chứng minh Kafka/Spark đã hoạt động. Lần sau chạy lại sẽ tránh thêm event_id trùng trong file spool cùng ngày.
