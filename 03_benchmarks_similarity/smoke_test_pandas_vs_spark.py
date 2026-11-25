"""[D1] Smoke-test Pandas vs PySpark trên lát cắt METR-LA 100K–1M dòng (Nhóm 11).

Tác vụ đo (mỗi tác vụ tính từ lúc đọc Parquet tới khi có kết quả cuối):
    read_scan             đọc Parquet, đếm dòng và tổng speed
    groupby_sensor_hour   bỏ zero sentinel, trung bình speed theo (sensor_id, giờ)
    rolling_mean_60min    zero sentinel -> null, trung bình trượt 12 bước (60 phút) theo từng cảm biến

Nguồn (theo 02_pyspark_gigo/parquet_schema_contract.md):
    mặc định data/interim/metr_la_sample.parquet/ (output [K2], partition theo year_month),
    nếu chưa có thì data/interim/metr-la.parquet (output [T1]). --source nhận cả file lẫn thư mục.

Quy trình:
    1. Cắt N dòng đầu theo thứ tự thời gian (partition year_month tăng dần) thành file riêng cho mỗi mốc,
       chỉ giữ timestamp, sensor_id, speed. timestamp ghi dạng TIMESTAMP(MICROS) không múi giờ như contract.
       N không chia hết cho số cảm biến thì mốc thời gian cuối chỉ có một phần cảm biến, và phần đó phụ thuộc
       thứ tự sắp xếp của nguồn (K2 sắp sensor_id theo chuỗi, T1 theo thứ tự cột CSV).
    2. Mỗi cặp (engine, mốc) chạy trong một tiến trình con riêng để RAM không cộng dồn giữa các lần đo.
    3. Mỗi tác vụ chạy 1 lần khởi động (không ghi) rồi --repeats lần đo.
    4. Đối chiếu checksum giữa Pandas và Spark, ghi CSV kết quả và in bảng tóm tắt (median, speedup).

Chạy:
    python 03_benchmarks_similarity/smoke_test_pandas_vs_spark.py
    python 03_benchmarks_similarity/smoke_test_pandas_vs_spark.py --sizes 100000 1000000 --repeats 5
    SPARK_DRIVER_MEMORY=2g python 03_benchmarks_similarity/smoke_test_pandas_vs_spark.py --master "local[4]"

Mã thoát: 0 nếu mọi lần chạy thành công và checksum hai engine khớp, 1 nếu ngược lại.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(PROJECT / "02_pyspark_gigo"))

from benchmark_profiler import append_csv, environment_info, measure  # noqa: E402

TASKS = ("read_scan", "groupby_sensor_hour", "rolling_mean_60min")
ENGINES = ("pandas", "spark")
DEFAULT_SIZES = (100_000, 500_000, 1_000_000)
ROLLING_STEPS = 12
CHECKSUM_REL_TOL = 1e-9
RESULT_PREFIX = "RESULT_JSON "
TASK_START_PREFIX = "TASK_START "
FIELDS = [
    "run_id", "run_started_utc", "engine", "task", "rows", "repeat",
    "wall_s", "peak_rss_mb", "peak_python_mb", "peak_children_mb", "baseline_rss_mb", "delta_rss_mb", "samples",
    "result_rows", "checksum", "slice_file", "slice_bytes",
    "spark_master", "spark_driver_memory", "spark_shuffle_partitions",
    "platform", "machine", "cpu_logical", "cpu_physical", "total_ram_gb",
    "python", "pandas", "pyarrow", "pyspark", "java",
]


# --------------------------------------------------------------------------- #
# Chuẩn bị lát cắt dữ liệu
# --------------------------------------------------------------------------- #
def default_source() -> Path:
    for candidate in (PROJECT / "data/interim/metr_la_sample.parquet", PROJECT / "data/interim/metr-la.parquet"):
        if candidate.exists():
            return candidate
    return PROJECT / "data/interim/metr_la_sample.parquet"


def slice_name(source: Path) -> str:
    return source.name.removesuffix(".parquet")


def iter_source_batches(source: Path):
    """Đọc theo thứ tự thời gian: file đơn đọc tuần tự, thư mục partition đọc từng year_month tăng dần."""
    import pyarrow.dataset as ds

    columns = ["timestamp", "sensor_id", "speed"]
    if source.is_file():
        yield from ds.dataset(source, format="parquet").to_batches(columns=columns, batch_size=200_000)
        return
    parts = sorted(p for p in source.iterdir() if p.is_dir() and p.name.startswith("year_month="))
    if not parts:
        raise ValueError(f"{source} không phải file Parquet hoặc thư mục partition year_month=")
    for part in parts:
        files = sorted(part.glob("*.parquet"))
        yield from ds.dataset(files, format="parquet").to_batches(columns=columns, batch_size=200_000)


def prepare_slice(source: Path, rows: int, slice_dir: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    target = slice_dir / f"{slice_name(source)}_{rows}.parquet"
    if target.exists() and target.stat().st_mtime >= source.stat().st_mtime \
            and pq.ParquetFile(target).metadata.num_rows == rows:
        return target
    schema = pa.schema([("timestamp", pa.timestamp("us")), ("sensor_id", pa.string()), ("speed", pa.float64())])
    slice_dir.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    written = 0
    with pq.ParquetWriter(tmp, schema, compression="snappy", coerce_timestamps="us") as writer:
        for batch in iter_source_batches(source):
            table = pa.Table.from_batches([batch]).slice(0, rows - written).cast(schema)
            writer.write_table(table)
            written += table.num_rows
            if written >= rows:
                break
    if written < rows:
        tmp.unlink(missing_ok=True)
        raise ValueError(f"{source} chỉ có {written:,} dòng, không đủ {rows:,}")
    tmp.replace(target)
    return target


# --------------------------------------------------------------------------- #
# Tác vụ Pandas
# --------------------------------------------------------------------------- #
def pandas_task(task: str, path: Path) -> tuple[int, float]:
    import pandas as pd

    df = pd.read_parquet(path, engine="pyarrow")
    if task == "read_scan":
        return len(df), float(df["speed"].sum())
    if task == "groupby_sensor_hour":
        valid = df[df["speed"] > 0]
        out = valid.groupby([valid["sensor_id"], valid["timestamp"].dt.hour])["speed"].mean()
        return len(out), float(out.sum())
    if task == "rolling_mean_60min":
        df["speed"] = df["speed"].where(df["speed"] > 0)
        df = df.sort_values(["sensor_id", "timestamp"], kind="stable")
        out = df.groupby("sensor_id", sort=False)["speed"].rolling(ROLLING_STEPS, min_periods=1).mean()
        return len(out), float(out.sum())
    raise ValueError(task)


# --------------------------------------------------------------------------- #
# Tác vụ PySpark
# --------------------------------------------------------------------------- #
def spark_task(spark, task: str, path: Path) -> tuple[int, float]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = spark.read.parquet(str(path))
    if task == "read_scan":
        row = df.agg(F.count(F.lit(1)), F.sum("speed")).first()
    elif task == "groupby_sensor_hour":
        out = (df.filter(F.col("speed") > 0)
                 .groupBy("sensor_id", F.hour("timestamp").alias("hour"))
                 .agg(F.avg("speed").alias("avg_speed")))
        row = out.agg(F.count(F.lit(1)), F.sum("avg_speed")).first()
    elif task == "rolling_mean_60min":
        window = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-(ROLLING_STEPS - 1), 0)
        out = (df.withColumn("speed", F.when(F.col("speed") > 0, F.col("speed")))
                 .withColumn("rolling_speed", F.avg("speed").over(window)))
        row = out.agg(F.count(F.lit(1)), F.sum("rolling_speed")).first()
    else:
        raise ValueError(task)
    return int(row[0]), float(row[1] or 0.0)


# --------------------------------------------------------------------------- #
# Tiến trình con: một engine, một lát cắt
# --------------------------------------------------------------------------- #
def emit(record: dict) -> None:
    print(RESULT_PREFIX + json.dumps(record, ensure_ascii=False), flush=True)


def mark_start(task: str) -> None:
    """Báo tác vụ sắp chạy để tiến trình cha biết tác vụ nào đang dở nếu tiến trình con bị dừng."""
    print(TASK_START_PREFIX + task, flush=True)


def worker(args: argparse.Namespace) -> int:
    path = Path(args.worker_slice)
    spark = None
    if args.worker_engine == "spark":
        from spark_env_check import build_spark  # cấu hình Spark chuẩn của [K1]

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        mark_start("spark_startup")
        with measure() as m:
            spark = build_spark(args.master, args.driver_memory)
            spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
            spark.range(1).count()
        emit({"task": "spark_startup", "repeat": 1, "result_rows": 0, "checksum": 0.0, **m.as_dict()})

    run = (lambda t: spark_task(spark, t, path)) if spark is not None else (lambda t: pandas_task(t, path))
    try:
        for task in args.tasks:
            mark_start(task)
            run(task)  # khởi động: nạp page cache, JIT/codegen của Spark
            for repeat in range(1, args.repeats + 1):
                with measure() as m:
                    result_rows, checksum = run(task)
                emit({"task": task, "repeat": repeat, "result_rows": result_rows, "checksum": checksum, **m.as_dict()})
    finally:
        if spark is not None:
            spark.stop()
    return 0


def run_worker(args: argparse.Namespace, engine: str, slice_path: Path) -> list[dict]:
    cmd = [sys.executable, str(Path(__file__).resolve()),
           "--worker-engine", engine, "--worker-slice", str(slice_path),
           "--tasks", *args.tasks, "--repeats", str(args.repeats),
           "--master", args.master, "--driver-memory", args.driver_memory,
           "--shuffle-partitions", str(args.shuffle_partitions)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    records = [json.loads(line[len(RESULT_PREFIX):]) for line in proc.stdout.splitlines()
               if line.startswith(RESULT_PREFIX)]
    if proc.returncode != 0:
        tail = "\n".join(proc.stderr.strip().splitlines()[-15:])
        raise RuntimeError(f"{engine} trên {slice_path.name} lỗi (mã {proc.returncode}):\n{tail}")
    return records


# --------------------------------------------------------------------------- #
# Tổng hợp
# --------------------------------------------------------------------------- #
def checksum_report(rows: list[dict]) -> list[str]:
    problems = []
    by_key: dict[tuple, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in rows:
        if r["task"] in TASKS:
            by_key[(r["rows"], r["task"])][r["engine"]].add((r["result_rows"], r["checksum"]))
    for (size, task), engines in sorted(by_key.items()):
        values = [v for vals in engines.values() for v in vals]
        counts = {c for c, _ in values}
        sums = [s for _, s in values]
        ref = max(abs(s) for s in sums) or 1.0
        rel = (max(sums) - min(sums)) / ref
        status = "khớp" if len(counts) == 1 and rel <= CHECKSUM_REL_TOL else "LỆCH"
        if status == "LỆCH":
            problems.append(f"{task} @ {size:,}: số dòng {sorted(counts)}, sai lệch tương đối {rel:.2e}")
        print(f"  {task:<22} {size:>10,}  result_rows={sorted(counts)}  rel_diff={rel:.2e}  {status}")
    return problems


def print_summary(rows: list[dict]) -> None:
    med = defaultdict(list)
    for r in rows:
        med[(r["rows"], r["task"], r["engine"])].append(r)
    print(f"\n{'rows':>10} {'task':<22} {'pandas_s':>9} {'spark_s':>9} {'speedup':>8} {'pandas_MB':>10} {'spark_MB':>9}")
    for size in sorted({r["rows"] for r in rows}):
        for task in ("spark_startup", *TASKS):
            p = med.get((size, task, "pandas"), [])
            s = med.get((size, task, "spark"), [])
            if not p and not s:
                continue
            pw = statistics.median(x["wall_s"] for x in p) if p else math.nan
            sw = statistics.median(x["wall_s"] for x in s) if s else math.nan
            pm = statistics.median(x["peak_rss_mb"] for x in p) if p else math.nan
            sm = statistics.median(x["peak_rss_mb"] for x in s) if s else math.nan
            speed = pw / sw if p and s else math.nan
            print(f"{size:>10,} {task:<22} {pw:>9.3f} {sw:>9.3f} {speed:>8.2f} {pm:>10.1f} {sm:>9.1f}")
    print("speedup = median(pandas) / median(spark); > 1 nghĩa là Spark nhanh hơn. MB là đỉnh RSS cây tiến trình.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=None,
                        help="File hoặc thư mục Parquet long-format (mặc định: output [K2], nếu chưa có thì [T1])")
    parser.add_argument("--slice-dir", type=Path, default=PROJECT / "data/interim/bench")
    parser.add_argument("--output", type=Path, default=HERE / "smoke_benchmark_results.csv")
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    parser.add_argument("--tasks", nargs="+", choices=TASKS, default=list(TASKS))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    parser.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--worker-engine", choices=ENGINES, help=argparse.SUPPRESS)
    parser.add_argument("--worker-slice", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1 or any(s < 1 for s in args.sizes):
        parser.error("--repeats và --sizes phải dương")

    if args.worker_engine:
        return worker(args)

    args.source = args.source or default_source()
    if not args.source.exists():
        parser.error(f"Không thấy dữ liệu nguồn {args.source}. Chạy [T1]/[K2] trước hoặc truyền --source.")

    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    env = environment_info()
    print(f"run_id={run_id}  nguồn={args.source}  cpu={env['cpu_logical']}  ram={env['total_ram_gb']} GB  "
          f"spark={args.master}/{args.driver_memory}")

    all_rows: list[dict] = []
    failures: list[str] = []
    for size in sorted(args.sizes):
        slice_path = prepare_slice(args.source, size, args.slice_dir)
        for engine in args.engines:
            print(f"- {engine:<6} {size:>10,} dòng ...", flush=True)
            try:
                records = run_worker(args, engine, slice_path)
            except RuntimeError as exc:
                failures.append(str(exc))
                print(f"  LỖI: {exc}")
                continue
            for rec in records:
                all_rows.append({
                    "run_id": run_id, "run_started_utc": started, "engine": engine, "rows": size,
                    "slice_file": slice_path.name, "slice_bytes": slice_path.stat().st_size,
                    "spark_master": args.master if engine == "spark" else "",
                    "spark_driver_memory": args.driver_memory if engine == "spark" else "",
                    "spark_shuffle_partitions": args.shuffle_partitions if engine == "spark" else "",
                    **env, **rec,
                })

    append_csv(args.output, all_rows, FIELDS)
    print(f"\nĐã ghi {len(all_rows)} dòng vào {args.output}")
    print("\nĐối chiếu checksum Pandas vs Spark:")
    failures += checksum_report(all_rows) if len(args.engines) == 2 else []
    print_summary(all_rows)
    if failures:
        print("\nCó lỗi:\n" + "\n".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
