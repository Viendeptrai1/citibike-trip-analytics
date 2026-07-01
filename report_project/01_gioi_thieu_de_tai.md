# Chương 1. Giới thiệu đề tài

## 1.1. Bối cảnh và lý do chọn đề tài

Trình bày sự gia tăng của dữ liệu chuyến đi đô thị, nhu cầu lưu trữ và phân tích dữ
liệu lớn, cùng lý do dữ liệu NYC Citi Bike phù hợp để khảo sát một kiến trúc
Data Lakehouse.

TODO(citation): Dẫn nguồn chính thức về dữ liệu Citi Bike và tài liệu liên quan.

## 1.2. Phát biểu bài toán

Mô tả vấn đề cần giải quyết: xây dựng một hệ thống có khả năng thu thập, lưu trữ,
làm sạch, tổng hợp và trực quan hóa dữ liệu chuyến đi Citi Bike trên một kiến trúc
Data Lakehouse thống nhất.

Nêu rõ các khó khăn:

- Khối lượng dữ liệu tăng theo thời gian.
- Dữ liệu nguồn có thể thiếu hoặc không hợp lệ.
- Cần bảo toàn dữ liệu thô nhưng vẫn tạo được dữ liệu tin cậy cho phân tích.
- Cần phục vụ cả xử lý phân tán, truy vấn SQL và dashboard.

## 1.3. Mục tiêu đề tài

### 1.3.1. Mục tiêu tổng quát

Thiết kế và triển khai hệ thống Data Lakehouse để xử lý và phân tích dữ liệu NYC
Citi Bike bằng các công nghệ mã nguồn mở.

### 1.3.2. Mục tiêu cụ thể

- Thu thập và lưu trữ dữ liệu nguồn trên object storage.
- Xây dựng pipeline Bronze–Silver–Gold bằng Apache Spark.
- Quản lý các bảng dữ liệu bằng Delta Lake.
- Xây dựng quy tắc kiểm tra và làm sạch dữ liệu.
- Tạo các bảng tổng hợp phục vụ truy vấn và dashboard.
- Đánh giá kết quả xử lý và rút ra nhận xét từ dữ liệu.

## 1.4. Câu hỏi nghiên cứu và câu hỏi phân tích

### 1.4.1. Câu hỏi về hệ thống

1. Kiến trúc Data Lakehouse có thể tổ chức dữ liệu Citi Bike từ dữ liệu thô đến
   dữ liệu phân tích như thế nào?
2. Các lớp Bronze, Silver và Gold cải thiện khả năng truy vết, chất lượng và khả
   năng sử dụng dữ liệu ra sao?
3. Kiến trúc đề xuất có hỗ trợ đầy đủ luồng dữ liệu từ nguồn đến lớp phục vụ phân
   tích và bảo đảm khả năng tái lập hay không?

### 1.4.2. Câu hỏi phân tích dữ liệu

1. Nhu cầu sử dụng xe thay đổi như thế nào theo ngày và giờ?
2. Các trạm và cặp trạm nào có lưu lượng cao?
3. Hành vi của thành viên và khách vãng lai khác nhau như thế nào?
4. Mức sử dụng các loại xe có khác biệt ra sao?

## 1.5. Đối tượng và phạm vi

### 1.5.1. Đối tượng

- Dữ liệu lịch sử chuyến đi NYC Citi Bike.
- Quy trình xử lý dữ liệu lớn theo kiến trúc Data Lakehouse.

### 1.5.2. Phạm vi

- Batch processing; chưa xử lý streaming thời gian thực.
- Phân tích dữ liệu trong các tháng được cấu hình cho thực nghiệm.
- Không xây dựng mô hình dự báo hoặc tối ưu vận hành trạm trong phiên bản hiện tại.

TODO(review): Chốt chính xác khoảng thời gian dữ liệu dùng trong báo cáo.

## 1.6. Phương pháp thực hiện

- Nghiên cứu tài liệu về Data Lakehouse và các công nghệ liên quan.
- Khảo sát cấu trúc và chất lượng dữ liệu nguồn.
- Thiết kế kiến trúc và mô hình dữ liệu phân lớp.
- Hiện thực pipeline, kiểm thử và truy vấn phân tích.
- Thực nghiệm, trực quan hóa và thảo luận kết quả.

## 1.7. Đóng góp của đề tài

Liệt kê các sản phẩm có thể kiểm chứng: kiến trúc, mã nguồn pipeline, quy tắc chất
lượng dữ liệu, các bảng Gold, truy vấn SQL, dashboard và tài liệu tái lập hệ thống.

## 1.8. Cấu trúc báo cáo

Tóm tắt vai trò của Chương 2 đến Chương 5, mỗi chương từ một đến hai câu.
