"""Convert wide public traffic CSVs to long, Spark-friendly Parquet incrementally."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ns")),
    ("sensor_id", pa.string()),
    ("speed_mph", pa.float64()),
])


def convert(source: Path, destination: Path, chunk_rows: int = 1000) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    temp.unlink(missing_ok=True)
    total_rows = total_sensors = 0
    writer = None
    try:
        for wide in pd.read_csv(source, index_col=0, chunksize=chunk_rows, low_memory=False):
            wide.index = pd.to_datetime(wide.index, errors="raise")
            wide.columns = wide.columns.map(str)
            nrows, nsensors = wide.shape
            # Build Arrow columns directly in row-major order to avoid a large
            # intermediate pandas melt and preserve the source float64 values.
            matrix = wide.to_numpy(dtype="float64", copy=False)
            table = pa.Table.from_arrays([
                pa.array(np.repeat(wide.index.to_numpy(), nsensors), type=pa.timestamp("ns")),
                pa.array(np.tile(wide.columns.to_numpy(dtype=str), nrows), type=pa.string()),
                pa.array(matrix.reshape(-1), type=pa.float64(), from_pandas=True),
            ], schema=SCHEMA)
            if writer is None:
                metadata = dict(table.schema.metadata or {})
                metadata.update({
                    b"source_file": source.name.encode(),
                    b"description": b"Long-format traffic speeds; one row per timestamp and sensor",
                    b"speed_unit": b"mph",
                })
                writer = pq.ParquetWriter(temp, table.schema.with_metadata(metadata), compression="snappy")
            writer.write_table(table, row_group_size=table.num_rows)
            total_rows += nrows
            total_sensors = nsensors
        if writer is None:
            raise ValueError(f"No records found in {source}")
        writer.close()
        writer = None
        temp.replace(destination)
    except Exception:
        if writer is not None:
            writer.close()
        temp.unlink(missing_ok=True)
        raise
    parquet = pq.ParquetFile(destination)
    return {
        "source": str(source), "output": str(destination),
        "source_time_rows": total_rows, "sensors": total_sensors,
        "expected_records": total_rows * total_sensors,
        "parquet_records": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "size_bytes": destination.stat().st_size,
        "schema": str(parquet.schema_arrow),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path(__file__).resolve().parents[1] / "data/raw")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[1] / "data/interim")
    parser.add_argument("--chunk-rows", type=int, default=1000)
    args = parser.parse_args()
    if args.chunk_rows < 1:
        parser.error("--chunk-rows must be positive")
    for name in ("METR-LA", "PEMS-BAY"):
        result = convert(args.input_dir / f"{name}.csv", args.output_dir / f"{name.lower()}.parquet", args.chunk_rows)
        print(f"{result['source']} -> {result['output']}")
        print(f"  {result['parquet_records']:,} rows in {result['row_groups']} row groups; expected {result['expected_records']:,}; {result['size_bytes']:,} bytes")
        print(result["schema"])


if __name__ == "__main__":
    main()
