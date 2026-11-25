# [D3] Kết quả benchmark Pandas vs PySpark (pems_bay)

Sinh tự động bởi `03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py`, run `2befd6f5` (2026-11-25T10:09:08+00:00).

- Máy: aarch64, 4 CPU logic, 3.82 GB RAM, Python 3.10.12, pandas 2.3.3, pyarrow 25.0.1, PySpark 3.5.9.
- Spark: `local[*]`, driver `2g`, khởi động SparkSession median 3.08 s (không tính vào thời gian tác vụ).
- Mỗi ô là median của 5 lần đo sau 1 lần khởi động. RAM là đỉnh RSS cả cây tiến trình (Python + JVM với Spark).

| Số dòng | Tác vụ | Pandas (s) | PySpark (s) | Speedup | Pandas RAM (MB) | PySpark RAM (MB) |
|---:|---|---:|---:|---:|---:|---:|
| 1,000,000 | Đọc + quét | 0.015 | 0.127 | 0.12 | 228 | 527 |
| 1,000,000 | GroupBy cảm biến × giờ | 0.049 | 0.192 | 0.26 | 292 | 655 |
| 1,000,000 | Rolling mean 60 phút | 0.206 | 0.374 | 0.55 | 386 | 827 |
| 5,000,000 | Đọc + quét | 0.065 | 0.123 | 0.53 | 576 | 527 |
| 5,000,000 | GroupBy cảm biến × giờ | 0.210 | 0.269 | 0.78 | 773 | 893 |
| 5,000,000 | Rolling mean 60 phút | 1.134 | 1.450 | 0.78 | 1123 | 1161 |
| 10,000,000 | Đọc + quét | 0.124 | 0.126 | 0.99 | 839 | 549 |
| 10,000,000 | GroupBy cảm biến × giờ | 0.417 | 0.298 | 1.40 | 1371 | 993 |
| 10,000,000 | Rolling mean 60 phút | 2.506 | 2.435 | 1.03 | 2154 | 1638 |
| 16,937,700 | Đọc + quét | 0.199 | 0.155 | 1.29 | 1343 | 566 |
| 16,937,700 | GroupBy cảm biến × giờ | 0.837 | 0.498 | 1.68 | 2450 | 1153 |
| 16,937,700 | Rolling mean 60 phút | mem_limit | 4.179 | — | > 3217 | 2185 |

Speedup = Pandas / PySpark, > 1 nghĩa là PySpark nhanh hơn. Trạng thái khác `ok`: `mem_limit` vượt ngân sách RAM đặt bằng `--mem-limit-mb`, `killed` bị hệ điều hành dừng (OOM killer), `oom` MemoryError/OutOfMemoryError, `timeout` quá `--timeout-s`, `skipped` không chạy vì đã lỗi ở mốc nhỏ hơn.

Biểu đồ:

- `figures/benchmark_pems_bay_wall_time.png`
- `figures/benchmark_pems_bay_peak_ram.png`
- `figures/benchmark_pems_bay_speedup.png`
