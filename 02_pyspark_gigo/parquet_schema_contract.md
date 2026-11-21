# [K2] Data Contract — Parquet long-format giao thông (METR-LA / PEMS-BAY)

> Người phụ trách: Nguyễn Duy Khang (`26410055`) · Script: `02_pyspark_gigo/csv_to_partitioned_parquet.py` · Nguồn: CSV wide của `[T1]` (`data/raw/METR-LA.csv`)
> Áp dụng cho mọi bước đọc/ghi Parquet giao thông từ Tuần 5 (GIGO, benchmark `[D1]`, vectorizer `[K3]`).

## 1. Schema

Mỗi dòng là **một lần đo của một cảm biến tại một mốc 5 phút**.

| Cột | Spark SQL | Parquet (vật lý) | pandas (pyarrow) | Đơn vị / Ý nghĩa | Null |
| :--- | :--- | :--- | :--- | :--- | :---: |
| `timestamp` | `TIMESTAMP_NTZ` | `INT64 TIMESTAMP(MICROS, isAdjustedToUTC=false)` | `datetime64[us]` | Mốc đo 5 phút, **không múi giờ** (xem §2) | Không |
| `sensor_id` | `STRING` | `BYTE_ARRAY (UTF8)` | `object` | Mã cảm biến gốc (vd `773869`), khớp `01_data_ingestion/graph_sensor_locations.csv` | Không |
| `speed` | `DOUBLE` | `DOUBLE` | `float64` | Vận tốc trung bình 5 phút, **mph**, giá trị thô | Có* |
| `hour` | `INT` | `INT32` | `int32` | Giờ trong ngày `0–23`, lấy từ `timestamp` | Không |
| `day_of_week` | `INT` | `INT32` | `int32` | **ISO: `1` = Thứ Hai … `7` = Chủ Nhật** | Không |
| `year_month` | `STRING` (cột partition) | thư mục `year_month=YYYY-MM/` | `category` | Tháng của `timestamp`, dạng `yyyy-MM` | Không |

\* Nguồn METR-LA hiện không có ô trống (`null = 0`); cột vẫn cho phép null để GIGO Tuần 5 đánh dấu giá trị bị loại.

## 2. Quy tắc bắt buộc

1. **`timestamp` giữ nguyên nhãn thời gian của nguồn, không gắn múi giờ và không quy đổi.** Spark dùng `TIMESTAMP_NTZ`; pandas giữ `datetime64` naive; **không** `tz_localize`/`tz_convert`. Hai bộ dữ liệu ghi thời gian khác nhau:
   - **METR-LA:** lưới đều 5 phút liên tục (`34.272` mốc, không trùng, không hở), kể cả `2012-03-11 02:00–02:55`, khung giờ không tồn tại ở Los Angeles do chuyển DST. Gán múi giờ sẽ lỗi hoặc làm lệch giờ.
   - **PEMS-BAY:** giờ địa phương thật, **thiếu đúng 1 giờ** `2017-03-12 02:00–02:55` do chuyển DST (bước nhảy `01:55 → 03:00`). Cửa sổ trượt (`[K3]`) hoặc nội suy (GIGO) đi qua mốc này thực chất dài 65 phút.
2. **Không dùng timestamp `NANOS` hoặc `INT96`.** Spark 3.5 không đọc được `TIMESTAMP(NANOS)`, mặc định của `pyarrow`/`pandas.to_parquet` (lỗi `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`). Ghi bằng pyarrow thì dùng `pq.write_table(..., coerce_timestamps="us")`.
3. **`speed` giữ nguyên giá trị thô.** `0.0` là tín hiệu mất kết nối của cảm biến (METR-LA: `575.302` ô = `8,11%`). Bước này chưa làm sạch; việc thay thế/loại thuộc pipeline GIGO Tuần 5. Đọc CSV phải parse số chính xác (Spark mặc định chính xác; pandas cần `float_precision="round_trip"`).
4. **Partition theo `year_month`**, nén **snappy**, mỗi tháng **1 file**, sắp xếp trong file theo `(timestamp, sensor_id)`. Không partition theo `sensor_id`/`hour` vì sẽ tạo quá nhiều file nhỏ.
5. **Đường dẫn cục bộ dùng URI `file://` tuyệt đối** khi máy có `HADOOP_CONF_DIR`. Nếu không, Spark hiểu đường dẫn tương đối là đường dẫn HDFS. Script `[K2]` tự chuyển đổi.
6. **Tên file đầu ra:** `data/interim/<dataset>_sample.parquet/` (vd `metr_la_sample.parquet`). Không commit (`.gitignore` đã chặn `*.parquet`, `data/interim/*`).

## 3. Đọc dữ liệu

```python
# PySpark: lọc theo partition chỉ đọc thư mục tháng cần thiết
df = spark.read.parquet("file:///.../data/interim/metr_la_sample.parquet").where("year_month = '2012-05'")

# pandas / pyarrow
import pandas as pd
df = pd.read_parquet("data/interim/metr_la_sample.parquet", filters=[("year_month", "=", "2012-05")])
```

## 4. Kết quả chuyển đổi METR-LA (18/11/2026)

Môi trường: Spark 3.5.9 `local[*]`, 6 vCPU, `driver.memory=4g`.

| Chỉ số | Giá trị |
| :--- | :--- |
| Bảng nguồn | `34.272` mốc × `207` cảm biến (`2012-03-01 00:00` → `2012-06-27 23:55`) |
| Số dòng long-format | `7.094.304` (= `34.272 × 207`) |
| Partition | `2012-03` (1.848.096) · `2012-04` (1.788.480) · `2012-05` (1.848.096) · `2012-06` (1.609.632) |
| Thời gian Spark (đọc CSV + unpivot + ghi) | `19,2 – 22,3 s` (3 lần chạy) |
| Dung lượng | CSV `69,4 MB` → Parquet `9,8 MB` (**nhỏ hơn 7,1×**) |
| Kiểm chứng | Giá trị `speed` khớp **100%** từng ô với CSV; `hour`, `day_of_week`, `year_month` khớp pandas; pandas/pyarrow đọc được |

PEMS-BAY dùng cùng script (`--input data/raw/PEMS-BAY.csv --output data/interim/pems_bay_sample.parquet`): `52.116 × 325` → `16.937.700` dòng, 6 partition (`2017-01` → `2017-06`), `27,2 s`, CSV `81,7 MB` → Parquet `25,4 MB` (3,2×), contract check PASS.

Lựa chọn thứ tự sắp xếp: sắp theo `(sensor_id, timestamp)` làm file tăng lên `20,9 MB` (gấp 2,1×). Spark Window và pandas `groupby` đều tự sắp xếp lại theo cảm biến, nên thứ tự đó không mang lại lợi ích cho `[K3]`/`[D1]`.
