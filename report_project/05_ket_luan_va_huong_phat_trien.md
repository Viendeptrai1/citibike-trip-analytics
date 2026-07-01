# Chương 5. Kết luận và hướng phát triển

## 5.1. Kết luận

Tóm tắt bài toán, phương pháp, hệ thống đã xây dựng và các kết quả định lượng quan
trọng. Không giới thiệu nội dung mới trong phần này.

TODO(data): Điền kết quả nổi bật sau khi hoàn thành Chương 4.

## 5.2. Đóng góp đạt được

- Kiến trúc Data Lakehouse gồm các thành phần lưu trữ, xử lý và phục vụ dữ liệu
  được tách biệt rõ ràng.
- Môi trường triển khai có khả năng tái lập nhất quán.
- Pipeline xử lý dữ liệu theo Bronze–Silver–Gold.
- Bộ quy tắc chất lượng dữ liệu và kiểm thử.
- Các bảng phân tích, truy vấn SQL và dashboard.

TODO(review): Chỉ giữ các đóng góp đã có bằng chứng trong mã nguồn hoặc thực nghiệm.

## 5.3. Hạn chế

- Chỉ xử lý theo lô.
- Thực nghiệm phụ thuộc tài nguyên một máy.
- Phạm vi thời gian dữ liệu còn giới hạn.
- Chưa có orchestration và incremental ingestion hoàn chỉnh.
- Chưa kết hợp dữ liệu ngoại sinh như thời tiết hoặc sự kiện.

## 5.4. Hướng phát triển

- Bổ sung công cụ orchestration để tự động kích hoạt pipeline khi dữ liệu của
  tháng mới được công bố. Quy trình định kỳ gồm thu thập dữ liệu, xử lý
  Bronze–Silver–Gold, kiểm tra chất lượng và chỉ cập nhật lớp phục vụ dashboard
  khi toàn bộ bước xử lý hoàn tất thành công.
- Chuyển từ cơ chế tái xử lý toàn bộ sang incremental ingestion theo tháng. Pipeline
  cần nhận diện dữ liệu đã xử lý, chỉ nạp phần dữ liệu mới và bảo đảm tính
  idempotent khi một batch được thực thi lại.
- Bổ sung giám sát trạng thái batch, thời gian xử lý, số lượng bản ghi, chất lượng
  đầu ra và cảnh báo khi pipeline thất bại.
- Tối ưu file, partition và hiệu năng truy vấn.
- Mở rộng dữ liệu nhiều năm và kết hợp dữ liệu thời tiết.
- Nghiên cứu streaming, dự báo nhu cầu hoặc phát hiện bất thường.
- Hoàn thiện quản trị dữ liệu, lineage và kiểm soát truy cập.
