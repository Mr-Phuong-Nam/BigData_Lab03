## 1. Chương trình RegenerateTaxiData
- **Nguyên do:** hàm đọc csv files của đổi tượng `DataStreamReader` sẽ đọc các file trong thư mục theo thứ tự ngày giờ bị sữa đổi (modified time) của file. Vì các file ban đầu có thời gian tạo khá lộn xộn ảnh hưởng đến luồng dữ liệu đầu vào của chương trình.
- **Mục đích:** tạo lại các file để thời gian tạo file theo thứ tự tăng dần.
- **Cách thực hiện:** 
    + Sử dụng cặp i-j với i là giờ, j là phút để đọc tuần tự các file.
    + Viết lại file này ra thư mục
<img src="./images/nam/image.png" width="700">   
- Ngoài ra chương trình còn tạo thêm một file chứa dòng dữ liệu ảo ở ngày 02 để ngắt watermark (sẽ được giải thích ở phần sau).

## 2. Chương trình Task_1_StreamSimulator.py
Cấu trúc (4 phần chính): chương trình được viết dưới dạng class để dễ dàng tái sử dụng trong các task khác.
- Hàm `setup_environment`: tạo các biến môi trường cho chương trình spark.
- Hàm `initialize_spark`: khởi tạo spark session.
- Hàm `define_schema`: định nghĩa schema cho dữ liệu đầu vào.
    - Vì có 2 loại dữ liệu là green và yellow taxi có 2 schema khác nhau nên ta sẽ đưa tất cả các kiểu dữ liệu về StringType.
    - Ngoài ra vì 2 loại dữ liệu này có số cột khác nhau nên ta sẽ lấy số cột tối đa là 22 của green taxi. Các cột còn lại của yellow taxi sẽ được đưa về null.
- Hàm `start_streaming`: tạo luồng dữ liệu đọc từ thư mục chứa dữ liệu.
    <img src="./images/nam/image copy.png" width="700">
    - Hàm `read_stream`: khởi tạo luồng dữ liệu.
    <image src="./images/nam/image copy 2.png" width="700">
        - `option('mode', 'PERMISSIVE')`: dùng để đọc tất cả các dòng dữ liệu không đúng với schema (yellow taxi chỉ có 19 cột).
        - `option('maxFilesPerTrigger', 1)`: đọc 1 file mỗi lần file.
        - `option('latestFirst', 'false')`: đọc file theo thứ tự tăng dần của thời gian tạo file.
    - Hàm `query`: thực hiện các thao tác trên Unbounded Table và xuất ra kết quả
    <image src="./images/nam/image copy 3.png" width="700">
        