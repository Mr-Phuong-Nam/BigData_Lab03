import time
input_path = "../taxi-data/"
header = "part-2015-12-01-"

# Đọc và ghi lại dữ liệu từ các file csv để các các file có thứ tự modified time tăng dần
for i in range(0, 24):
    for j in range(0, 60):
        print(i, j)
        file = header + str(i).zfill(2) + str(j).zfill(2) + ".csv"
        with open(input_path + file, "r") as f:
            lines = f.readlines()
            f.close()
        with open(input_path + file, "w") as f:
            for line in lines:
                f.write(line)
            f.close()
        time.sleep(0.01)

# Tạo thêm một dòng dữ liệu mới vào ngày 2015-12-02 đẻ ngắt watermark
time.sleep(1)
with open(input_path + "part-2015-12-02-0101.csv", "w") as f:
    f.write("yellow,2,2015-12-02 00:01:06,2015-12-02 01:01:01,6,.85,-73.9881591796875,40.75579833984375,1,N,-73.990318298339844,40.745960235595703,2,5,0.5,0.5,0,0,0.3,6.3")
    f.close()