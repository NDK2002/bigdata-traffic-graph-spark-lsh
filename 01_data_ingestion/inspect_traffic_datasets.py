"""Inspect DCRNN-format METR-LA and PEMS-BAY HDF5 files (Week 4, T1)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import h5py
import pandas as pd


def inspect_hdf(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"Dataset not found: {path}")
    info = {"file": str(path), "size_bytes": path.stat().st_size, "keys": []}
    with h5py.File(path, "r") as h5:
        def visitor(name, obj):
            if isinstance(obj, h5py.Dataset):
                info["keys"].append({"key": name, "shape": list(obj.shape), "dtype": str(obj.dtype)})
        h5.visititems(visitor)
    try:
        with pd.HDFStore(path, mode="r") as store:
            for key in store.keys():
                frame = store.select(key, start=0, stop=1)
                info.setdefault("pandas", []).append({
                    "key": key, "columns": len(frame.columns),
                    "column_sample": [str(c) for c in frame.columns[:5]],
                    "index_sample": [str(v) for v in frame.index[:1]],
                })
    except (ImportError, ValueError, OSError) as exc:
        info["pandas_read_note"] = str(exc)
    return info


def inspect_csv(path: Path) -> dict:
    """Inspect the public Zenodo CSV release without loading the full matrix."""
    if not path.is_file():
        raise FileNotFoundError(f"Dataset not found: {path}")
    sample = pd.read_csv(path, nrows=5)
    first = sample.columns[0]
    sample[first] = pd.to_datetime(sample[first], errors="coerce")
    # Read only the timestamp column for exact range and row count.
    timestamps = pd.to_datetime(pd.read_csv(path, usecols=[first])[first], errors="coerce")
    return {
        "file": str(path), "size_bytes": path.stat().st_size,
        "rows": len(timestamps), "sensors": len(sample.columns) - 1,
        "timestamp_column": first,
        "min_timestamp": str(timestamps.min()), "max_timestamp": str(timestamps.max()),
        "sensor_id_sample": [str(c) for c in sample.columns[1:6]],
        "sample_dtype": str(pd.read_csv(path, nrows=1).iloc[:, 1].dtype),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", type=Path, help="HDF5/CSV paths; defaults to data/raw data files")
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = args.files or sorted([
        *(root / "data" / "raw").glob("*.h5"),
        root / "data" / "raw" / "METR-LA.csv",
        root / "data" / "raw" / "PEMS-BAY.csv",
    ])
    if not files:
        parser.error("No .h5/.csv files found. Place METR-LA/PEMS-BAY under data/raw or pass paths.")
    report = [inspect_csv(p) if p.suffix.lower() == ".csv" else inspect_hdf(p) for p in files]
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
