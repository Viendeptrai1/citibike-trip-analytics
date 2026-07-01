# Chương 6. Kết luận và hướng phát triển

## 6.1. Kết luận

Tóm tắt bài toán, phương pháp, hệ thống đã xây dựng và các kết quả định lượng quan
trọng. Không giới thiệu nội dung mới trong phần này.

TODO(data): Điền kết quả nổi bật sau khi hoàn thành Chương 5.

## 6.2. Đóng góp đạt được

- Kiến trúc Data Lakehouse gồm các thành phần lưu trữ, xử lý và phục vụ dữ liệu
  được tách biệt rõ ràng.
- Môi trường triển khai có khả năng tái lập nhất quán.
- Pipeline xử lý dữ liệu theo Bronze–Silver–Gold.
- Bộ quy tắc chất lượng dữ liệu và kiểm thử.
- Các bảng phân tích, truy vấn SQL và dashboard.

TODO(review): Chỉ giữ các đóng góp đã có bằng chứng trong mã nguồn hoặc thực nghiệm.

## 6.3. Hạn chế

- Chỉ xử lý theo lô.
- Thực nghiệm phụ thuộc tài nguyên một máy.
- Phạm vi thời gian dữ liệu còn giới hạn.
- Chưa có orchestration và incremental ingestion hoàn chỉnh.
- Chưa kết hợp dữ liệu ngoại sinh như thời tiết hoặc sự kiện.

## 6.4. Hướng phát triển

- Bổ sung orchestration và giám sát pipeline.
- Xây dựng incremental ingestion và xử lý idempotent.
- Tối ưu file, partition và hiệu năng truy vấn.
- Mở rộng dữ liệu nhiều năm và kết hợp dữ liệu thời tiết.
- Nghiên cứu streaming, dự báo nhu cầu hoặc phát hiện bất thường.
- Hoàn thiện quản trị dữ liệu, lineage và kiểm soát truy cập.
