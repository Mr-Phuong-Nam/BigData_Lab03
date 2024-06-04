## 1. Chương trình RegenerateTaxiData
- **Nguyên do:** hàm đọc csv files của đổi tượng `DataStreamReader` sẽ đọc các file trong thư mục theo thứ tự của thời gian tạo (modified time) của file. Vì các file ban đầu có thời gian tạo khá lộn xộn ảnh hưởng đến luồng dữ liệu đầu vào của chương trình.
- **Mục đích:** tạo lại các file để thời gian tạo file đúng theo thứ tự tăng dần. Từ đó các file đi vào stream đúng theo thứ tự trong thư mục. Việc này sẽ làm cho dòng dữ liệu đầu vào của chương trình giống như dữ liệu thực tế đang đổ vào.
- **Cách thực hiện:** 
    + Sử dụng cặp i-j với i là giờ, j là phút để đọc tuần tự các file.
    + Viết lại file này ra thư mục
<img src="./images/nam/image.png" width="700">   
- Ngoài ra chương trình còn tạo thêm một file chứa dòng dữ liệu ảo ở ngày 02 tháng 12 để ngắt watermark (sẽ được giải thích ở phần sau).

## 2. Chương trình Task_1_StreamSimulator.py
Cấu trúc (4 phần chính): chương trình được viết dưới dạng class để dễ dàng tái sử dụng trong các task khác.
- Hàm `setup_environment`: tạo các biến môi trường cho chương trình spark.
- Hàm `initialize_spark`: khởi tạo spark session.
- Hàm `define_schema`: định nghĩa schema cho dữ liệu đầu vào.
    - Vì có 2 loại dữ liệu là green và yellow taxi có 2 schema khác nhau nên ta sẽ đưa tất cả các kiểu dữ liệu về StringType.
    - Ngoài ra vì 2 loại dữ liệu này có số cột khác nhau nên ta sẽ lấy số cột tối đa là 22 của green taxi. Các cột bị thiếu của yellow taxi sẽ được đưa về null.
- Hàm `start_streaming`: tạo luồng dữ liệu đọc từ thư mục chứa dữ liệu.
    <img src="./images/nam/image copy.png" width="700">
    - Hàm `read_stream`: khởi tạo luồng dữ liệu.
    <image src="./images/nam/image copy 2.png" width="700">
        - `option('mode', 'PERMISSIVE')`: dùng để đọc tất cả các dòng dữ liệu không đúng với schema (yellow taxi chỉ có 19 cột).
        - `option('maxFilesPerTrigger', 1)`: số file đọc tối đa trong mỗi trigger
        - `option('latestFirst', 'false')`: đọc file theo thứ tự tăng dần của thời gian tạo file.
    - Hàm `query`: thực hiện các thao tác trên Unbounded Table và xuất ra kết quả
    <image src="./images/nam/image copy 3.png" width="700">
- Output: 
<image src="./images/nam/image copy 5.png" width="700">

## 3. Chương trình Task_2_EventCount
Thực hiện overwriting hàm `query` của chương trình Task_1_StreamSimulator để thực hiện các thao tác trên Unbounded Table và xuất ra kết quả.
<image src="./images/nam/image copy 4.png" width="700">
- Bởi vì chương trình chỉ quan tâm đến sự kiện dựa trên dropoff_datetime nên ta sẽ chỉ lấy cột này để thực hiện các thao tác.
- `watermark("dropoff_datetime", "30 minute")`: đây là phần bắt buôc để luồng dữ liẹu có thể chạy trong thời gian thực. 
    - `30 minute`: là thời gian delay của watermark. Các dòng dữ liệu nằm tới sau với thời gian bé hơn `max event time - delay (watermark)` sẽ bị bỏ qua
    - Các window nằm trong watermark sẽ được giữ lại dưới dạng intermediate state để tiếp tục thực hiện update.
    - Các window nằm ngoài watermark sẽ được xuất ra kết quả dưới dạng append mode.
- Hàm `foreach_batch_function`: dùng để in ra kết quả của từng window đươc realse từ watermark vào folder tương ứng duới dạng json file.
<image src="./images/nam/image copy 6.png" width="700">
- Output từ console: ta có thể thấy một số batch không có dữ liệu do watermark đã giữ lại dữ liệu để chờ thời gian delay trôi qua. Khi window này trượt qua watermark thì dữ liệu sẽ được xuất ra.
<image src="./images/nam/image copy 7.png" width="700">
- Output từ folder: 
<image src="./images/nam/image copy 8.png" width="700">

## 4. Chương trình Task_3_RegionEventCount
Chương trình này hoàn toàn tương tự như chương trình [EventCount](#3-chương-trình-task_2_eventcount) đã trình bày ở trên. Điểm khác biệt duy nhất đó là việc thêm filter để lọc ra dữ liệu mong muốn.

Để làm được việc này, nhóm đã thực hiên như sau:

**Xác định giới hạn cho từng vùng**: Từ bounding box đã cho có thể xác định được $x_{min}, x_{max}, y_{min}, y_{max}$ với $x$ là kinh độ và $y$ là vĩ độ.

**Xác định vùng trả khách của từng records**:

Gọi $x, y$ lần lượt là kinh độ và vĩ độ của nơi trả khách. Điểm đó thuộc một vùng khi:

<!-- $(x^{i}_{min} \leq x \leq  x^{i}_{max}) \wedge (y^{i}_{min} \leq y \leq y^{i}_{max})$ với $i \in \{'goldman', 'citigroup'\}$ -->

$(x^{r}_{min} \leq x \leq  x^{r}_{max}) \wedge (y^{r}_{min} \leq y \leq y^{r}_{max})$

Với các vùng $r \in \{'goldman', 'citigroup'\}$

Các điểm không thuộc 2 vùng trên được ghi nhận là $'other'$.

**Tạo một cột tên `headquarter` để ghi nhận lại kết quả**: Với mỗi records, các cột `col_1`, `col_9`, `col_10`, `col_11`, `col_12` được cho vào hàm `in_region` để xác định vùng trả khách. Trong hàm này, cột `col_1` cho biết loại xe từ đó có thể xác định các thuộc tính khác dựa vào schema.
- Với loại xe màu vàng: `x` là cột `col_11` và  `y` là cột `col_12`.
- Với loại xe màu xanh: `x` là cột `col_9` và  `y` là cột `col_10`.

Hàm `in_region` trả về tên vùng hoặc `other` nếu không thuộc 2 vùng cho trước.

**Thêm `headquarter` vào groupBy**: Ngoài groupBy theo giờ, ta cần đếm số lượng trả khách tại vùng đó, vì vậy cần thêm `headquarter` vào.

Ngoài ra, phần output cũng được đổi thành loại file `csv` cho phù hợp với yêu cầu đề bài.

Phần còn lại của chương trình tương tự như [EventCount](#3-chương-trình-task_2_eventcount).

**Output từ console**

<image src='./images/Phuc/1.png' width='500'>