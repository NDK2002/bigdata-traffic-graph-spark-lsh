"""Audit raw METR-LA and PEMS-BAY traffic speeds for GIGO issues (T3)."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def audit_file(path: Path) -> tuple[dict, pd.DataFrame]:
    # Files are wide (time x sensor), with timestamps in the first column.
    df = pd.read_csv(path, index_col=0, low_memory=False)
    timestamps = pd.to_datetime(df.index, errors="coerce")
    df.index = timestamps
    df = df.apply(pd.to_numeric, errors="coerce")
    values = df.to_numpy(dtype=np.float64, copy=False)
    finite = np.isfinite(values)
    # In these traffic files, zero is a missing-sensor sentinel. Exclude zero,
    # non-finite, and physically impossible speeds from the IQR reference set;
    # report each excluded class separately below.
    observed = finite & (values > 0) & (values <= 80)
    valid = values[observed]
    if not valid.size:
        raise ValueError(f"No physically plausible positive speeds in {path}")
    q1, q3 = np.quantile(valid, [0.25, 0.75])
    iqr = q3 - q1
    low_iqr, high_iqr = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    zero = values == 0
    missing = np.isnan(values)
    physical = finite & ((values < 0) | (values > 80))
    iqr_outlier = observed & ((values < low_iqr) | (values > high_iqr))
    total = values.size
    stats = {
        "dataset": path.stem,
        "rows": int(df.shape[0]),
        "sensors": int(df.shape[1]),
        "start": str(df.index.min()),
        "end": str(df.index.max()),
        "total_cells": int(total),
        "observed_speed_count": int(observed.sum()),
        "observed_speed_pct": float(observed.sum() / total * 100),
        "zero_count": int(zero.sum()),
        "zero_pct": float(zero.sum() / total * 100),
        "nan_count": int(missing.sum()),
        "nan_pct": float(missing.sum() / total * 100),
        "nonfinite_count": int((~finite).sum()),
        "physical_outlier_count": int(physical.sum()),
        "physical_outlier_pct": float(physical.sum() / total * 100),
        "iqr_outlier_count": int(iqr_outlier.sum()),
        "iqr_outlier_pct": float(iqr_outlier.sum() / valid.size * 100),
        "iqr_outlier_total_pct": float(iqr_outlier.sum() / total * 100),
        "min": float(np.min(valid)),
        "q1": float(q1),
        "median": float(np.median(valid)),
        "q3": float(q3),
        "max": float(np.max(valid)),
        "iqr": float(iqr),
        "iqr_lower_fence": float(low_iqr),
        "iqr_upper_fence": float(high_iqr),
    }
    per_sensor = []
    for column in df.columns:
        s = df[column].to_numpy(dtype=np.float64, copy=False)
        ok = np.isfinite(s)
        sv = s[ok & (s > 0) & (s <= 80)]
        sq1, sq3 = np.quantile(sv, [0.25, 0.75]) if sv.size else (np.nan, np.nan)
        siqr = sq3 - sq1
        per_sensor.append({
            "sensor_id": str(column), "zero_count": int((s == 0).sum()),
            "nan_count": int(np.isnan(s).sum()), "nonfinite_count": int((~ok).sum()),
            "physical_outlier_count": int((ok & ((s < 0) | (s > 80))).sum()),
            "observed_speed_count": int(sv.size),
            "q1": float(sq1), "q3": float(sq3), "iqr": float(siqr),
            "iqr_lower_fence": float(sq1 - 1.5 * siqr) if sv.size else np.nan,
            "iqr_upper_fence": float(sq3 + 1.5 * siqr) if sv.size else np.nan,
            "iqr_outlier_count": int(((s > 0) & (s <= 80) & ((s < sq1 - 1.5 * siqr) | (s > sq3 + 1.5 * siqr))).sum()) if sv.size else 0,
        })
    detail = pd.DataFrame(per_sensor)
    detail["zero_pct"] = detail["zero_count"] / len(df) * 100
    detail["nan_pct"] = detail["nan_count"] / len(df) * 100
    detail["physical_outlier_pct"] = detail["physical_outlier_count"] / len(df) * 100
    hourly = []
    hour_values = df.index.hour
    for hour in range(24):
        block = values[hour_values == hour]
        if not block.size:
            continue
        block_finite = np.isfinite(block)
        denominator = block.size
        hourly.append({
            "recorded_hour": hour,
            "records": int(block.shape[0]),
            "zero_count": int((block == 0).sum()),
            "zero_pct": float((block == 0).sum() / denominator * 100),
            "nan_count": int(np.isnan(block).sum()),
            "nan_pct": float(np.isnan(block).sum() / denominator * 100),
            "physical_outlier_count": int((block_finite & ((block < 0) | (block > 80))).sum()),
            "physical_outlier_pct": float((block_finite & ((block < 0) | (block > 80))).sum() / denominator * 100),
        })
    return stats, detail, pd.DataFrame(hourly)


def markdown_table(frame: pd.DataFrame) -> str:
    cols = list(frame.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
    for row in frame.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(f"{v:.4f}" if isinstance(v, (float, np.floating)) else str(v) for v in row) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", type=Path)
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("gigo_audit_report.md"))
    parser.add_argument("--detail-dir", type=Path, help="Directory for per-sensor CSV tables")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    files = args.files or [project / "data/raw/METR-LA.csv", project / "data/raw/PEMS-BAY.csv"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    detail_dir = args.detail_dir or args.output.parent
    detail_dir.mkdir(parents=True, exist_ok=True)
    report = [
        "# Báo cáo kiểm toán chất lượng dữ liệu giao thông (GIGO)", "",
        "> Kiểm toán dữ liệu thô trước tiền xử lý; vận tốc tính bằng mph. Ngưỡng vật lý: `< 0` hoặc `> 80 mph`.",
        "> Tỷ lệ 0/NaN tính trên tổng ô cảm biến-thời gian. Giá trị 0 được xem là sentinel mất tín hiệu. Q1/Q3/IQR tính trên vận tốc hữu hạn trong (0, 80] mph; zero sentinel, NaN và ngoại lai vật lý được loại khỏi phân bố tham chiếu IQR và báo riêng. Ngoại lai IQR dùng hàng rào Q1 − 1.5×IQR / Q3 + 1.5×IQR.",
        "> Cờ IQR là ứng viên cần kiểm tra, không đồng nghĩa lỗi chắc chắn và không tự động bị xóa; vận tốc thấp có thể là ùn tắc thật. Giờ trong ngày là giờ ghi trong timestamp nguồn (không tự gán múi giờ).",
        "> Nguồn: METR-LA và PEMS-BAY CSV, [Zenodo record 5146275](https://zenodo.org/records/5146275); dữ liệu gốc được giữ nguyên trong `data/raw/`.", "",
    ]
    for path in files:
        stats, detail, hourly = audit_file(path)
        detail_path = detail_dir / f"{path.stem.lower()}_sensor_audit.csv"
        hourly_path = detail_dir / f"{path.stem.lower()}_hourly_audit.csv"
        detail.to_csv(detail_path, index=False, encoding="utf-8-sig")
        hourly.to_csv(hourly_path, index=False, encoding="utf-8-sig")
        report += [f"## {stats['dataset']}", "", "| Chỉ số | Kết quả |", "|---|---:|"]
        labels = [
            ("Số mốc thời gian", "rows"), ("Số cảm biến", "sensors"),
            ("Khoảng thời gian", None), ("Tổng số ô", "total_cells"),
            ("Vận tốc quan sát hợp lệ (0 < speed ≤ 80 mph)", "observed_speed_count"),
            ("Tỷ lệ vận tốc quan sát hợp lệ (%)", "observed_speed_pct"),
            ("Giá trị 0", "zero_count"), ("Tỷ lệ 0 (%)", "zero_pct"),
            ("NaN", "nan_count"), ("Tỷ lệ NaN (%)", "nan_pct"),
            ("Giá trị không hữu hạn (NaN/±Inf)", "nonfinite_count"),
            ("Ngoại lai vật lý (<0 hoặc >80 mph)", "physical_outlier_count"),
            ("Tỷ lệ ngoại lai vật lý (%)", "physical_outlier_pct"),
            ("Ngoại lai IQR trong vận tốc quan sát hợp lệ", "iqr_outlier_count"),
            ("Tỷ lệ ngoại lai IQR trong vận tốc quan sát hợp lệ (%)", "iqr_outlier_pct"),
            ("Tỷ lệ ngoại lai IQR trên tổng số ô (%)", "iqr_outlier_total_pct"),
            ("Min", "min"), ("Q1", "q1"), ("Median", "median"), ("Q3", "q3"),
            ("Max", "max"), ("IQR = Q3 − Q1", "iqr"),
            ("Hàng rào IQR dưới", "iqr_lower_fence"), ("Hàng rào IQR trên", "iqr_upper_fence"),
        ]
        for label, key in labels:
            value = f"{stats['start']} đến {stats['end']}" if key is None else stats[key]
            report.append(f"| {label} | {value:.6f} |" if isinstance(value, float) else f"| {label} | {value} |")
        report += ["", f"Thống kê từng cảm biến: `{detail_path.name}`.",
                   f"Phân bố chất lượng theo giờ trong ngày: `{hourly_path.name}`.", "",
                   "Cảm biến có nhiều nhất giá trị 0:", "",
                   markdown_table(detail.nlargest(5, "zero_count")[["sensor_id", "zero_count", "zero_pct"]]), "",
                   "Cảm biến có nhiều nhất NaN:", "",
                   markdown_table(detail.nlargest(5, "nan_count")[["sensor_id", "nan_count", "nan_pct"]]), ""]
        report += ["Tỷ lệ số 0 theo giờ ghi nhận trong dữ liệu:", "",
                   markdown_table(hourly[["recorded_hour", "zero_count", "zero_pct"]]), "",
                   "Tỷ lệ ngoại lai vật lý theo giờ ghi nhận trong dữ liệu:", "",
                   markdown_table(hourly[["recorded_hour", "physical_outlier_count", "physical_outlier_pct"]]), ""]
    args.output.write_text("\n".join(report), encoding="utf-8")
    print(f"Report: {args.output}")
    for p in sorted(detail_dir.glob("*_sensor_audit.csv")):
        print(f"Sensor details: {p}")
    for p in sorted(detail_dir.glob("*_hourly_audit.csv")):
        print(f"Hourly details: {p}")


if __name__ == "__main__":
    main()
