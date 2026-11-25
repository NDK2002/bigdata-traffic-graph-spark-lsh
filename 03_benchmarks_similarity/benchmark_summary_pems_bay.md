# [D3] Kết quả benchmark Pandas vs PySpark (pems_bay)

Sinh tự động bởi `03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py`, run `53585bbd` (2026-11-24T15:39:12+00:00).

- Máy: aarch64, 4 CPU logic, 3.82 GB RAM, Python 3.10.12, pandas 2.3.3, pyarrow 25.0.1, PySpark 3.5.9.
- Spark: `local[*]`, driver `2g`, khởi động SparkSession median 3.38 s (không tính vào thời gian tác vụ).
- Mỗi ô là median của 3 lần đo sau 1 lần khởi động. RAM là đỉnh RSS cả cây tiến trình (Python + JVM với Spark).

| Số dòng | Tác vụ | Pandas (s) | PySpark (s) | Speedup | Pandas RAM (MB) | PySpark RAM (MB) |
|---:|---|---:|---:|---:|---:|---:|
| 1,000,000 | Đọc + quét | 0.017 | 0.098 | 0.17 | 249 | 533 |
| 1,000,000 | GroupBy cảm biến × giờ | 0.049 | 0.182 | 0.27 | 293 | 660 |
| 1,000,000 | Rolling mean 60 phút | 0.189 | 0.390 | 0.49 | 401 | 826 |
| 5,000,000 | Đọc + quét | 0.063 | 0.165 | 0.38 | 546 | 537 |
| 5,000,000 | GroupBy cảm biến × giờ | 0.213 | 0.360 | 0.59 | 866 | 960 |
| 5,000,000 | Rolling mean 60 phút | 1.131 | 3.256 | 0.35 | 1117 | 1671 |
| 10,000,000 | Đọc + quét | 0.160 | 0.250 | 0.64 | 845 | 529 |
| 10,000,000 | GroupBy cảm biến × giờ | 0.559 | 1.340 | 0.42 | 1396 | 1394 |
| 10,000,000 | Rolling mean 60 phút | 2.646 | 3.815 | 0.69 | 2162 | 1671 |
| 16,937,700 | Đọc + quét | 0.240 | 0.167 | 1.44 | 1305 | 526 |
| 16,937,700 | GroupBy cảm biến × giờ | 0.921 | 0.490 | 1.88 | 2480 | 1140 |
| 16,937,700 | Rolling mean 60 phút | killed | 4.176 | — | > 3129 | 1931 |

Speedup = Pandas / PySpark, > 1 nghĩa là PySpark nhanh hơn. Trạng thái khác `ok`: `mem_limit` vượt ngân sách RAM đặt bằng `--mem-limit-mb`, `killed` bị hệ điều hành dừng (OOM killer), `oom` MemoryError/OutOfMemoryError, `timeout` quá `--timeout-s`, `skipped` không chạy vì đã lỗi ở mốc nhỏ hơn.

Biểu đồ:

- `figures/benchmark_pems_bay_wall_time.png`
- `figures/benchmark_pems_bay_peak_ram.png`
- `figures/benchmark_pems_bay_speedup.png`
