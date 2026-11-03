SE363 — BỔ SUNG TỰ ĐỘNG THU THẬP RSS CHO COLLECTOR TRÊN UBUNTU

1) Giải nén NỘI DUNG ZIP vào thư mục ~/SE363_Auto_Collector/ (không đè collector.py, không thay .env)
2) Kiểm tra:
   python rss_se363.py --self-test
3) Thu thập:
   python rss_se363.py
4) Kiểm tra:
   cat data/reports/rss_ingestion_report.json
   wc -l data/raw/evidence_discovery.jsonl
   wc -l data/raw/posts_auto.jsonl

Nguồn xác minh tại https://suckhoecong.vn/rssMain.html :
- /thuc-pham-chuc-nang.rss : RSS bài viết, tài liệu tham khảo ứng viên
- /goc-nhin-quan-ly.rss : RSS bài viết chính sách/quản lý, evidence candidate
- /store.rss : RSS kênh Store, ứng viên nội dung quảng bá; KHÔNG mặc định là quảng cáo gốc

Các kênh RSS của nhà xuất bản được công bố cho sử dụng cá nhân / phi lợi nhuận; phải dẫn nguồn, không sao chép hay phát tán trái phép toàn văn. Kịch bản này chỉ lưu tiêu đề, tóm tắt RSS, URL, thời gian và nguồn. Nếu site chặn/429 thì ghi lỗi, không dùng proxy vượt chặn.

Tập tin được tạo là dữ liệu đầu vào ứng viên; cần xem nguồn gốc người phát biểu / kiểm tra thủ công trước khi dùng để đánh giá hệ thống. API chưa được thử trực tiếp từ môi trường tạo gói, do đó chưa bảo đảm số record thu được.

ĐỂ CÓ AD GỐC: dùng Meta Marketing API với tài khoản quảng cáo bạn được cấp quyền, hoặc nguồn/doanh nghiệp cấp phép. Đừng gán bài báo RSS làm quảng cáo gốc.
