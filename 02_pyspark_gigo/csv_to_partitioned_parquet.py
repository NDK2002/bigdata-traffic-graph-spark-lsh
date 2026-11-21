"""[K2] PySpark: METR-LA wide CSV ([T1]) -> long-format Parquet, partitioned by year_month.

Schema follows 02_pyspark_gigo/parquet_schema_contract.md:
    timestamp TIMESTAMP_NTZ | sensor_id STRING | speed DOUBLE | hour INT | day_of_week INT | year_month STRING (partition)

Run (repo root):
    python 02_pyspark_gigo/csv_to_partitioned_parquet.py
    python 02_pyspark_gigo/csv_to_partitioned_parquet.py --input data/raw/PEMS-BAY.csv --output data/interim/pems_bay.parquet

Local paths are converted to file:// URIs. When HADOOP_CONF_DIR is set, Spark otherwise resolves
relative paths on HDFS. Paths that already have a scheme (hdfs://, file://) are used unchanged.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_env_check import build_spark  # same folder; shared Spark config from [K1]

PROJECT = Path(__file__).resolve().parents[1]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
CONTRACT_COLUMNS = ["timestamp", "sensor_id", "speed", "hour", "day_of_week", "year_month"]


def to_uri(path: str) -> str:
    return path if "://" in path else Path(path).resolve().as_uri()


def local_size_bytes(path: str) -> int | None:
    if "://" in path and not path.startswith("file://"):
        return None
    p = Path(path.removeprefix("file://"))
    if p.is_file():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob("*.parquet")) if p.is_dir() else None


def wide_to_long(spark: SparkSession, csv_uri: str) -> tuple[DataFrame, int, int]:
    """Read the wide CSV (first column = timestamp, others = sensor ids) and unpivot it."""
    wide = spark.read.option("header", "true").option("inferSchema", "false").csv(csv_uri)
    ts_col, *sensor_cols = wide.columns  # header of column 0 is empty -> Spark names it _c0
    n_times = wide.count()

    wide = wide.select(
        F.to_timestamp_ntz(F.col(ts_col), F.lit(TS_FORMAT)).alias("timestamp"),
        *[F.col(f"`{c}`").cast("double").alias(c) for c in sensor_cols],
    )
    long = wide.unpivot("timestamp", sensor_cols, "sensor_id", "speed")
    return long, n_times, len(sensor_cols)


def add_contract_columns(df: DataFrame) -> DataFrame:
    return df.select(
        "timestamp",
        "sensor_id",
        "speed",
        F.hour("timestamp").alias("hour"),
        (F.weekday("timestamp") + 1).alias("day_of_week"),  # ISO: 1 = Monday ... 7 = Sunday
        F.date_format("timestamp", "yyyy-MM").alias("year_month"),
    )


def validate(spark: SparkSession, out_uri: str, expected_rows: int, out_local: Path | None) -> dict:
    back = spark.read.parquet(out_uri)
    if back.columns != CONTRACT_COLUMNS:
        raise AssertionError(f"columns {back.columns} != contract {CONTRACT_COLUMNS}")
    types = dict(back.dtypes)
    expected_types = {"timestamp": "timestamp_ntz", "sensor_id": "string", "speed": "double",
                      "hour": "int", "day_of_week": "int", "year_month": "string"}
    if types != expected_types:
        raise AssertionError(f"types {types} != contract {expected_types}")

    stats = back.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("timestamp").isNull().cast("int")).alias("null_ts"),
        F.sum(F.col("speed").isNull().cast("int")).alias("null_speed"),
        F.sum((F.col("speed") == 0).cast("int")).alias("zero_speed"),
        F.countDistinct("sensor_id").alias("sensors"),
        F.min("timestamp").alias("ts_min"),
        F.max("timestamp").alias("ts_max"),
    ).first().asDict()
    if stats["rows"] != expected_rows:
        raise AssertionError(f"rows {stats['rows']:,} != expected {expected_rows:,}")
    if stats["null_ts"]:
        raise AssertionError(f"{stats['null_ts']} timestamps failed to parse with format {TS_FORMAT}")
    stats["partitions"] = sorted(r.year_month for r in back.select("year_month").distinct().collect())

    if out_local is not None:  # check the physical Parquet type that pandas / pyarrow consumers will see
        import pyarrow.parquet as pq

        files = sorted(out_local.rglob("*.parquet"))
        ts_type = pq.ParquetFile(files[0]).schema.column(0).logical_type
        if "MICROS" not in str(ts_type).upper() or "ISADJUSTEDTOUTC=FALSE" not in str(ts_type).upper().replace(" ", ""):
            raise AssertionError(f"timestamp physical type {ts_type}; contract requires TIMESTAMP(MICROS, isAdjustedToUTC=false)")
        stats["files"] = len(files)
        stats["parquet_ts_type"] = "INT64 TIMESTAMP(MICROS, isAdjustedToUTC=false)"
    return stats


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", default=str(PROJECT / "data/raw/METR-LA.csv"), help="Wide CSV from [T1]")
    p.add_argument("--output", default=str(PROJECT / "data/interim/metr_la_sample.parquet"))
    p.add_argument("--master", default="local[*]")
    p.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    in_uri, out_uri = to_uri(args.input), to_uri(args.output)
    spark = build_spark(args.master, args.driver_memory)
    spark.conf.set("spark.app.name", "Nhom11-K2-csv_to_partitioned_parquet")
    try:
        start = time.perf_counter()
        long, n_times, n_sensors = wide_to_long(spark, in_uri)
        (
            add_contract_columns(long)
            .repartition("year_month")  # one file per month
            # Time-major order: each timestamp repeats in a run of N sensors, so Parquet encodes it ~2x
            # smaller than sensor-major, and time-range slices read contiguous row groups. year_month
            # must lead the sort key; otherwise the partitioned writer re-sorts and drops this order.
            .sortWithinPartitions("year_month", "timestamp", "sensor_id")
            .write.mode("overwrite")
            .partitionBy("year_month")
            .parquet(out_uri)
        )
        write_s = time.perf_counter() - start

        out_local = Path(out_uri.removeprefix("file://")) if out_uri.startswith("file://") else None
        stats = validate(spark, out_uri, n_times * n_sensors, out_local)
    finally:
        spark.stop()

    csv_bytes, pq_bytes = local_size_bytes(in_uri), local_size_bytes(out_uri)
    print("=" * 72)
    print(f"[K2] {in_uri}\n  -> {out_uri}")
    print(f"  wide shape          : {n_times:,} timestamps x {n_sensors} sensors")
    print(f"  long rows           : {stats['rows']:,} (= {n_times:,} x {n_sensors})")
    print(f"  sensors / partitions: {stats['sensors']} / {stats['partitions']}")
    print(f"  timestamp range     : {stats['ts_min']} -> {stats['ts_max']}")
    print(f"  speed null / zero   : {stats['null_speed']:,} / {stats['zero_speed']:,} "
          f"({stats['zero_speed'] / stats['rows']:.2%} zero = sensor dropout, kept raw)")
    if "files" in stats:
        print(f"  parquet files       : {stats['files']}  timestamp = {stats['parquet_ts_type']}")
    print(f"  Spark read+unpivot+write: {write_s:.2f}s (master={args.master}, driver={args.driver_memory})")
    if csv_bytes and pq_bytes:
        print(f"  size CSV -> Parquet : {csv_bytes / 2**20:.1f} MB -> {pq_bytes / 2**20:.1f} MB "
              f"({csv_bytes / pq_bytes:.1f}x smaller)")
    print("  contract check      : PASS")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
