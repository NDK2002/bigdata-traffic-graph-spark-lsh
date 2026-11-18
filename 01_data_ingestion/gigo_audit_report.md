# Báo cáo kiểm toán chất lượng dữ liệu giao thông (GIGO)

> Kiểm toán dữ liệu thô trước tiền xử lý; vận tốc tính bằng mph. Ngưỡng vật lý: `< 0` hoặc `> 80 mph`.
> Tỷ lệ 0/NaN tính trên tổng ô cảm biến-thời gian. Giá trị 0 được xem là sentinel mất tín hiệu. Q1/Q3/IQR tính trên vận tốc hữu hạn trong (0, 80] mph; zero sentinel, NaN và ngoại lai vật lý được loại khỏi phân bố tham chiếu IQR và báo riêng. Ngoại lai IQR dùng hàng rào Q1 − 1.5×IQR / Q3 + 1.5×IQR.
> Cờ IQR là ứng viên cần kiểm tra, không đồng nghĩa lỗi chắc chắn và không tự động bị xóa; vận tốc thấp có thể là ùn tắc thật. Giờ trong ngày là giờ ghi trong timestamp nguồn (không tự gán múi giờ).
> Nguồn: METR-LA và PEMS-BAY CSV, [Zenodo record 5146275](https://zenodo.org/records/5146275); dữ liệu gốc được giữ nguyên trong `data/raw/`.

## METR-LA

| Chỉ số | Kết quả |
|---|---:|
| Số mốc thời gian | 34272 |
| Số cảm biến | 207 |
| Khoảng thời gian | 2012-03-01 00:00:00 đến 2012-06-27 23:55:00 |
| Tổng số ô | 7094304 |
| Vận tốc quan sát hợp lệ (0 < speed ≤ 80 mph) | 6519002 |
| Tỷ lệ vận tốc quan sát hợp lệ (%) | 91.890649 |
| Giá trị 0 | 575302 |
| Tỷ lệ 0 (%) | 8.109351 |
| NaN | 0 |
| Tỷ lệ NaN (%) | 0.000000 |
| Giá trị không hữu hạn (NaN/±Inf) | 0 |
| Ngoại lai vật lý (<0 hoặc >80 mph) | 0 |
| Tỷ lệ ngoại lai vật lý (%) | 0.000000 |
| Ngoại lai IQR trong vận tốc quan sát hợp lệ | 781075 |
| Tỷ lệ ngoại lai IQR trong vận tốc quan sát hợp lệ (%) | 11.981512 |
| Tỷ lệ ngoại lai IQR trên tổng số ô (%) | 11.009889 |
| Min | 0.333333 |
| Q1 | 57.125000 |
| Median | 63.222222 |
| Q3 | 66.500000 |
| Max | 70.000000 |
| IQR = Q3 − Q1 | 9.375000 |
| Hàng rào IQR dưới | 43.062500 |
| Hàng rào IQR trên | 80.562500 |

Thống kê từng cảm biến: `metr-la_sensor_audit.csv`.
Phân bố chất lượng theo giờ trong ngày: `metr-la_hourly_audit.csv`.

Cảm biến có nhiều nhất giá trị 0:

| sensor_id | zero_count | zero_pct |
|---|---|---|
| 718076 | 6890 | 20.1039 |
| 716939 | 6464 | 18.8609 |
| 760987 | 5532 | 16.1415 |
| 716955 | 5374 | 15.6804 |
| 769418 | 5349 | 15.6075 |

Cảm biến có nhiều nhất NaN:

| sensor_id | nan_count | nan_pct |
|---|---|---|
| 773869 | 0 | 0.0000 |
| 767541 | 0 | 0.0000 |
| 767542 | 0 | 0.0000 |
| 717447 | 0 | 0.0000 |
| 717446 | 0 | 0.0000 |

Tỷ lệ số 0 theo giờ ghi nhận trong dữ liệu:

| recorded_hour | zero_count | zero_pct |
|---|---|---|
| 0 | 30878 | 10.4460 |
| 1 | 25635 | 8.6723 |
| 2 | 29214 | 9.8831 |
| 3 | 29119 | 9.8509 |
| 4 | 26020 | 8.8026 |
| 5 | 26237 | 8.8760 |
| 6 | 28993 | 9.8083 |
| 7 | 28092 | 9.5035 |
| 8 | 29751 | 10.0648 |
| 9 | 29750 | 10.0644 |
| 10 | 29090 | 9.8411 |
| 11 | 22540 | 7.6253 |
| 12 | 24340 | 8.2342 |
| 13 | 26342 | 8.9115 |
| 14 | 25142 | 8.5055 |
| 15 | 21165 | 7.1601 |
| 16 | 17912 | 6.0596 |
| 17 | 19681 | 6.6581 |
| 18 | 21289 | 7.2021 |
| 19 | 15839 | 5.3583 |
| 20 | 11719 | 3.9645 |
| 21 | 17646 | 5.9696 |
| 22 | 19870 | 6.7220 |
| 23 | 19038 | 6.4405 |

Tỷ lệ ngoại lai vật lý theo giờ ghi nhận trong dữ liệu:

| recorded_hour | physical_outlier_count | physical_outlier_pct |
|---|---|---|
| 0 | 0 | 0.0000 |
| 1 | 0 | 0.0000 |
| 2 | 0 | 0.0000 |
| 3 | 0 | 0.0000 |
| 4 | 0 | 0.0000 |
| 5 | 0 | 0.0000 |
| 6 | 0 | 0.0000 |
| 7 | 0 | 0.0000 |
| 8 | 0 | 0.0000 |
| 9 | 0 | 0.0000 |
| 10 | 0 | 0.0000 |
| 11 | 0 | 0.0000 |
| 12 | 0 | 0.0000 |
| 13 | 0 | 0.0000 |
| 14 | 0 | 0.0000 |
| 15 | 0 | 0.0000 |
| 16 | 0 | 0.0000 |
| 17 | 0 | 0.0000 |
| 18 | 0 | 0.0000 |
| 19 | 0 | 0.0000 |
| 20 | 0 | 0.0000 |
| 21 | 0 | 0.0000 |
| 22 | 0 | 0.0000 |
| 23 | 0 | 0.0000 |

## PEMS-BAY

| Chỉ số | Kết quả |
|---|---:|
| Số mốc thời gian | 52116 |
| Số cảm biến | 325 |
| Khoảng thời gian | 2017-01-01 00:00:00 đến 2017-06-30 23:55:00 |
| Tổng số ô | 16937700 |
| Vận tốc quan sát hợp lệ (0 < speed ≤ 80 mph) | 16936311 |
| Tỷ lệ vận tốc quan sát hợp lệ (%) | 99.991799 |
| Giá trị 0 | 521 |
| Tỷ lệ 0 (%) | 0.003076 |
| NaN | 0 |
| Tỷ lệ NaN (%) | 0.000000 |
| Giá trị không hữu hạn (NaN/±Inf) | 0 |
| Ngoại lai vật lý (<0 hoặc >80 mph) | 868 |
| Tỷ lệ ngoại lai vật lý (%) | 0.005125 |
| Ngoại lai IQR trong vận tốc quan sát hợp lệ | 1536246 |
| Tỷ lệ ngoại lai IQR trong vận tốc quan sát hợp lệ (%) | 9.070724 |
| Tỷ lệ ngoại lai IQR trên tổng số ô (%) | 9.069980 |
| Min | 3.000000 |
| Q1 | 62.100000 |
| Median | 65.300000 |
| Q3 | 67.500000 |
| Max | 80.000000 |
| IQR = Q3 − Q1 | 5.400000 |
| Hàng rào IQR dưới | 54.000000 |
| Hàng rào IQR trên | 75.600000 |

Thống kê từng cảm biến: `pems-bay_sensor_audit.csv`.
Phân bố chất lượng theo giờ trong ngày: `pems-bay_hourly_audit.csv`.

Cảm biến có nhiều nhất giá trị 0:

| sensor_id | zero_count | zero_pct |
|---|---|---|
| 404370 | 4 | 0.0077 |
| 404444 | 4 | 0.0077 |
| 404461 | 4 | 0.0077 |
| 405613 | 4 | 0.0077 |
| 407364 | 4 | 0.0077 |

Cảm biến có nhiều nhất NaN:

| sensor_id | nan_count | nan_pct |
|---|---|---|
| 400001 | 0 | 0.0000 |
| 400017 | 0 | 0.0000 |
| 400030 | 0 | 0.0000 |
| 400040 | 0 | 0.0000 |
| 400045 | 0 | 0.0000 |

Tỷ lệ số 0 theo giờ ghi nhận trong dữ liệu:

| recorded_hour | zero_count | zero_pct |
|---|---|---|
| 0 | 0 | 0.0000 |
| 1 | 308 | 0.0436 |
| 2 | 0 | 0.0000 |
| 3 | 0 | 0.0000 |
| 4 | 0 | 0.0000 |
| 5 | 0 | 0.0000 |
| 6 | 0 | 0.0000 |
| 7 | 0 | 0.0000 |
| 8 | 0 | 0.0000 |
| 9 | 0 | 0.0000 |
| 10 | 0 | 0.0000 |
| 11 | 0 | 0.0000 |
| 12 | 0 | 0.0000 |
| 13 | 0 | 0.0000 |
| 14 | 0 | 0.0000 |
| 15 | 0 | 0.0000 |
| 16 | 0 | 0.0000 |
| 17 | 0 | 0.0000 |
| 18 | 0 | 0.0000 |
| 19 | 81 | 0.0115 |
| 20 | 132 | 0.0187 |
| 21 | 0 | 0.0000 |
| 22 | 0 | 0.0000 |
| 23 | 0 | 0.0000 |

Tỷ lệ ngoại lai vật lý theo giờ ghi nhận trong dữ liệu:

| recorded_hour | physical_outlier_count | physical_outlier_pct |
|---|---|---|
| 0 | 11 | 0.0016 |
| 1 | 3 | 0.0004 |
| 2 | 7 | 0.0010 |
| 3 | 13 | 0.0018 |
| 4 | 10 | 0.0014 |
| 5 | 281 | 0.0398 |
| 6 | 50 | 0.0071 |
| 7 | 37 | 0.0052 |
| 8 | 9 | 0.0013 |
| 9 | 41 | 0.0058 |
| 10 | 33 | 0.0047 |
| 11 | 12 | 0.0017 |
| 12 | 0 | 0.0000 |
| 13 | 3 | 0.0004 |
| 14 | 141 | 0.0200 |
| 15 | 22 | 0.0031 |
| 16 | 29 | 0.0041 |
| 17 | 22 | 0.0031 |
| 18 | 10 | 0.0014 |
| 19 | 32 | 0.0045 |
| 20 | 49 | 0.0069 |
| 21 | 23 | 0.0033 |
| 22 | 22 | 0.0031 |
| 23 | 8 | 0.0011 |
