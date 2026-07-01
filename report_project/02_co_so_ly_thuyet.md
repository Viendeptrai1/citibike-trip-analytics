# Chương 2. Cơ sở lý thuyết

## 2.1. Dữ liệu lớn và xử lý dữ liệu phân tán

Trình bày đặc trưng của dữ liệu lớn có liên quan trực tiếp đến đề tài và lý do cần
xử lý phân tán. Tránh liệt kê lý thuyết không được sử dụng trong hệ thống.

## 2.2. Data Lake, Data Warehouse và Data Lakehouse

### 2.2.1. Data Lake

### 2.2.2. Data Warehouse

### 2.2.3. Data Lakehouse

### 2.2.4. So sánh các kiến trúc

TODO(citation): Dùng nguồn học thuật hoặc tài liệu gốc; lập bảng so sánh theo lưu
trữ, schema, giao dịch, workload và chi phí.

## 2.3. Kiến trúc Medallion

### 2.3.1. Bronze

### 2.3.2. Silver

### 2.3.3. Gold

Làm rõ Medallion là cách tổ chức mức độ tinh chế dữ liệu, không đồng nhất với các
lớp chức năng của toàn bộ hệ thống.

## 2.4. Object storage và định dạng dữ liệu dạng cột

Giải thích object storage tương thích S3, Parquet và lợi ích của lưu trữ dạng cột
đối với workload phân tích.

## 2.5. Apache Spark và mô hình xử lý phân tán

Trình bày khái niệm driver, executor, DataFrame, lazy evaluation, transformation,
action và partition ở mức cần thiết để giải thích cách pipeline hoạt động.

## 2.6. Table format và Delta Lake

### 2.6.1. Transaction log và giao dịch ACID

### 2.6.2. Schema enforcement và schema evolution

### 2.6.3. Versioning và time travel

Phân biệt định dạng file Parquet với table format Delta Lake.

## 2.7. Chất lượng dữ liệu

Trình bày các khía cạnh được dùng trong đề tài: completeness, validity, uniqueness
và consistency; liên hệ với các quy tắc kiểm tra `ride_id`, timestamp, tọa độ và
thông tin trạm.

## 2.8. Kỹ thuật biến đổi và tổng hợp dữ liệu

### 2.8.1. Đặc trưng thời gian

### 2.8.2. Khoảng cách Haversine

Trình bày công thức, biến số, đơn vị đo và giả định khi ước lượng khoảng cách giữa
hai tọa độ.

### 2.8.3. Phép nhóm và chỉ số tổng hợp

Giải thích cơ sở tạo các chỉ số về số chuyến, thời lượng trung bình, khoảng cách
trung bình và lưu lượng theo trạm.

## 2.9. Truy vấn phân tích và trực quan hóa dữ liệu

Trình bày vai trò của SQL query engine, BI dashboard và nguyên tắc chọn biểu đồ
phù hợp. Chi tiết cấu hình Trino và Superset được trình bày trong Chương 3.

## 2.10. Tiểu kết chương

Tổng hợp mối liên hệ giữa các lý thuyết và quyết định thiết kế ở Chương 3.
