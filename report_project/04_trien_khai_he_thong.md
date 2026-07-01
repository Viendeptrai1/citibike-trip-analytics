# Chương 4. Triển khai hệ thống

## 4.1. Môi trường triển khai

Nêu hệ điều hành, CPU, RAM, dung lượng, phiên bản Docker và các phiên bản công
nghệ. Các thông tin này giúp thực nghiệm có thể tái lập.

TODO(data): Ghi cấu hình máy và phiên bản image thực tế.

## 4.2. Tổ chức mã nguồn

Mô tả vai trò của các thư mục `config`, `docker`, `scripts`, `src`, `sql`,
`dashboard` và `tests`.

## 4.3. Triển khai hạ tầng bằng Docker Compose

### 4.3.1. MinIO

### 4.3.2. Spark

### 4.3.3. Trino

### 4.3.4. Superset

Chỉ trình bày cấu hình quan trọng đối với kiến trúc; lệnh thao tác chi tiết chuyển
sang phụ lục.

## 4.4. Hiện thực pipeline dữ liệu

### 4.4.1. Thu thập và nạp dữ liệu

### 4.4.2. Xây dựng lớp Bronze

### 4.4.3. Xây dựng lớp Silver

Mô tả ép kiểu timestamp, lọc bản ghi, tạo đặc trưng thời gian và tính khoảng cách.

### 4.4.4. Xây dựng lớp Gold

Giải thích logic tạo từng bảng tổng hợp, không sao chép toàn bộ mã nguồn.

## 4.5. Hiện thực kiểm tra chất lượng dữ liệu

Mô tả kiểm tra đầu vào/đầu ra, điều kiện pipeline thất bại và cách ghi log.

## 4.6. Hiện thực lớp truy vấn và dashboard

Mô tả cách Gold được truy vấn bằng Spark SQL hoặc Trino và được dùng trong
Superset.

## 4.7. Kiểm thử hệ thống

### 4.7.1. Unit test

### 4.7.2. Data quality test

### 4.7.3. Kiểm thử pipeline đầu cuối

TODO(data): Bổ sung số ca kiểm thử, số ca đạt và bằng chứng chạy.

## 4.8. Tiểu kết chương

Tóm tắt những thành phần đã hiện thực và dữ liệu đầu ra cho thực nghiệm.

