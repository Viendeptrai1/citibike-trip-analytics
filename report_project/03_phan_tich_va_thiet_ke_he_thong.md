# Chương 3. Phân tích và thiết kế hệ thống

## 3.1. Phân tích bộ dữ liệu NYC Citi Bike

### 3.1.1. Nguồn và phạm vi dữ liệu

### 3.1.2. Cấu trúc dữ liệu

Lập bảng data dictionary cho các cột quan trọng.

### 3.1.3. Vấn đề chất lượng dữ liệu

Mô tả các trường hợp thiếu trạm, thiếu tọa độ, timestamp sai, chuyến đi không có
thời lượng hợp lệ và khác biệt schema.

TODO(data): Thống kê số file, dung lượng, số bản ghi và tỷ lệ lỗi ban đầu.

## 3.2. Yêu cầu hệ thống

### 3.2.1. Yêu cầu chức năng

- Nạp dữ liệu theo tháng.
- Bảo toàn dữ liệu nguồn và metadata nạp.
- Làm sạch, chuẩn hóa và tạo đặc trưng.
- Tổng hợp dữ liệu phục vụ các câu hỏi phân tích.
- Hỗ trợ truy vấn SQL và dashboard.
- Kiểm tra tính hợp lệ của dữ liệu đầu ra.

### 3.2.2. Yêu cầu phi chức năng

- Có thể triển khai và tái lập nhất quán.
- Thành phần tách biệt và dễ thay thế.
- Dữ liệu có khả năng truy vết.
- Pipeline có kiểm thử và thông báo lỗi rõ ràng.

## 3.3. Kiến trúc tổng thể

TODO(figure): Sơ đồ từ Citi Bike ZIP/CSV → downloader → MinIO Raw → Spark →
Bronze/Silver/Gold Delta → Spark SQL/Trino → Superset.

Giải thích vai trò và ranh giới trách nhiệm của từng thành phần.

## 3.4. Lựa chọn công nghệ

Lập bảng gồm công nghệ, vai trò, lý do lựa chọn và phương án thay thế:
MinIO, Apache Spark, Delta Lake, Trino, Superset và Python. Phương thức đóng gói
và triển khai các thành phần được trình bày ở Chương 4.

## 3.5. Thiết kế lưu trữ và mô hình dữ liệu

### 3.5.1. Cấu trúc bucket và đường dẫn

### 3.5.2. Schema bảng Bronze

### 3.5.3. Schema bảng Silver

### 3.5.4. Schema các bảng Gold

Trình bày bảy bảng: `gold_daily_rides`, `gold_hourly_demand`,
`gold_top_start_stations`, `gold_top_end_stations`,
`gold_user_type_behavior`, `gold_bike_type_usage` và
`gold_station_od_pairs`.

## 3.6. Thiết kế luồng xử lý

### 3.6.1. Thu thập dữ liệu nguồn

### 3.6.2. Raw → Bronze

### 3.6.3. Bronze → Silver

### 3.6.4. Silver → Gold

### 3.6.5. Gold → lớp phục vụ phân tích

TODO(figure): Sequence diagram hoặc data-flow diagram thể hiện đầu vào, đầu ra và
điểm kiểm tra của từng bước.

## 3.7. Thiết kế quy tắc chất lượng dữ liệu

Lập bảng gồm tên quy tắc, lớp áp dụng, điều kiện, cách xử lý và lý do.

## 3.8. Thiết kế dashboard

Ánh xạ mỗi câu hỏi phân tích với bảng Gold, metric, dimension và loại biểu đồ.

## 3.9. Tiểu kết chương

Khẳng định thiết kế là cầu nối từ cơ sở lý thuyết sang phần hiện thực.
