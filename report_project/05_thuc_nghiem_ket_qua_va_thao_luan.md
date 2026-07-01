# Chương 5. Thực nghiệm, kết quả và thảo luận

## 5.1. Mục tiêu và kịch bản thực nghiệm

Xác định các nội dung cần đánh giá:

- Pipeline có chạy đầu cuối và tái lập được hay không.
- Dữ liệu Silver có cải thiện chất lượng so với dữ liệu đầu vào hay không.
- Các bảng Gold có trả lời được câu hỏi phân tích hay không.
- Thời gian và tài nguyên xử lý thay đổi thế nào theo quy mô dữ liệu.

## 5.2. Dữ liệu và cấu hình thực nghiệm

| Thuộc tính | Giá trị |
|---|---|
| Khoảng thời gian dữ liệu | TODO(data) |
| Số file đầu vào | TODO(data) |
| Dung lượng dữ liệu nén/giải nén | TODO(data) |
| Số bản ghi Bronze | TODO(data) |
| Cấu hình máy | TODO(data) |
| Cấu hình Spark | TODO(data) |

## 5.3. Kết quả vận hành pipeline

Lập bảng thời gian xử lý và số bản ghi đầu ra cho từng công đoạn. Phân biệt rõ kết
quả đo được với nhận xét.

TODO(data): Raw/Bronze/Silver/Gold record counts, runtime và trạng thái.

## 5.4. Kết quả đánh giá chất lượng dữ liệu

So sánh trước và sau làm sạch:

- Số và tỷ lệ bản ghi thiếu khóa.
- Số timestamp không hợp lệ.
- Số chuyến có `ended_at <= started_at`.
- Số bản ghi thiếu thông tin trạm hoặc tọa độ.
- Số bản ghi được giữ lại ở Silver.

TODO(data): Bảng thống kê và tiêu chí chấp nhận.

## 5.5. Kết quả phân tích dữ liệu

### 5.5.1. Xu hướng số chuyến theo thời gian

### 5.5.2. Nhu cầu theo ngày trong tuần và giờ

### 5.5.3. Các trạm và cặp OD phổ biến

### 5.5.4. So sánh thành viên và khách vãng lai

### 5.5.5. Mức sử dụng theo loại xe

Mỗi mục cần có hình/bảng, số liệu nổi bật và diễn giải thận trọng. Không suy ra quan
hệ nhân quả chỉ từ thống kê mô tả.

TODO(figure): Chèn biểu đồ được xuất từ dashboard hoặc truy vấn.

## 5.6. Dashboard

Trình bày bố cục dashboard, bộ lọc, ý nghĩa từng biểu đồ và một kịch bản sử dụng.

TODO(figure): Ảnh toàn cảnh dashboard và các biểu đồ quan trọng.

## 5.7. Thảo luận

### 5.7.1. Mức độ đáp ứng mục tiêu

Đối chiếu từng mục tiêu ở Mục 1.3 với bằng chứng thực nghiệm.

### 5.7.2. Ý nghĩa của kết quả phân tích

Giải thích kết quả có ý nghĩa gì đối với việc hiểu nhu cầu sử dụng Citi Bike.

### 5.7.3. Đánh đổi trong thiết kế

Thảo luận về batch processing, overwrite so với incremental, khả năng mở rộng,
giới hạn tài nguyên của môi trường thực nghiệm và độ phức tạp vận hành.

### 5.7.4. Đe dọa đối với tính hợp lệ

Nêu ảnh hưởng của phạm vi thời gian, dữ liệu thiếu, cách ước lượng khoảng cách,
cấu hình máy và dataset demo đến khả năng khái quát kết quả.

## 5.8. Tiểu kết chương

Tóm tắt các kết quả có bằng chứng và chuyển sang kết luận chung.
