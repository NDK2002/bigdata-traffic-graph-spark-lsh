"""[D3] Benchmark Pandas vs PySpark theo quy mô 1M → 17M dòng (Nhóm 11, Tuần 6).

Dùng lại tác vụ và tiến trình con của smoke_test_pandas_vs_spark.py ([D1]):
    read_scan, groupby_sensor_hour, rolling_mean_60min; mỗi cặp (engine, mốc) chạy trong tiến trình riêng,
    1 lần khởi động + --repeats lần đo, đối chiếu checksum Pandas/Spark.

Script này bổ sung phần cần cho quy mô lớn:
    - Nguồn mặc định PEMS-BAY từ [K2] (data/interim/pems_bay_sample.parquet/, 16.937.700 dòng).
      Mốc mặc định 1M, 5M, 10M và "all" (toàn bộ bộ dữ liệu, ~17M).
    - Giám sát RSS cả cây tiến trình con. Vượt --mem-limit-mb thì dừng và ghi status=mem_limit,
      dùng để mô phỏng máy có ngân sách RAM cố định khi tìm điểm gãy của Pandas.
    - --timeout-s cho mỗi cặp (engine, mốc). Tiến trình bị hệ điều hành kill (OOM killer) ghi status=killed,
      MemoryError/OutOfMemoryError ghi status=oom, lỗi khác ghi status=error.
    - Engine đã lỗi ở một mốc thì các mốc lớn hơn ghi status=skipped, không chạy.
    - Biểu đồ PNG và bảng Markdown cho Chương 3. --plot-only vẽ lại từ CSV mà không chạy đo.

Chạy:
    python 03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py
    python 03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py --driver-memory 4g --mem-limit-mb 6000
    python 03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py --sizes 1000000 all --repeats 5
    python 03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py --plot-only [--run-id <id>]

Mã thoát: 0 nếu không có lỗi ngoài dự kiến (status=error) và checksum hai engine khớp ở mọi mốc cả hai cùng chạy xong.
"""
from __future__ import annotations

import argparse
import math
import os
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import psutil

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(HERE))

import smoke_test_pandas_vs_spark as smoke  # noqa: E402
from benchmark_profiler import append_csv, environment_info  # noqa: E402

MB = 1024 * 1024
DEFAULT_SOURCE = PROJECT / "data/interim/pems_bay_sample.parquet"
DEFAULT_SIZES = ("1000000", "5000000", "10000000", "all")
FIELDS = smoke.FIELDS + ["dataset", "status", "error"]
FAIL_STATUSES = ("mem_limit", "timeout", "killed", "oom", "error")
TASK_LABELS = {
    "read_scan": "Đọc + quét",
    "groupby_sensor_hour": "GroupBy cảm biến × giờ",
    "rolling_mean_60min": "Rolling mean 60 phút",
}
ENGINE_LABELS = {"pandas": "Pandas", "spark": "PySpark"}
ENGINE_COLORS = {"pandas": "#2a78d6", "spark": "#eb6834"}
TASK_COLORS = {"read_scan": "#1baf7a", "groupby_sensor_hour": "#6250d6", "rolling_mean_60min": "#eda100"}
INK, INK_2, MUTED, GRID, AXIS = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"


# --------------------------------------------------------------------------- #
# Chạy tiến trình con có giám sát RAM / thời gian
# --------------------------------------------------------------------------- #
class Watchdog(threading.Thread):
    def __init__(self, proc: subprocess.Popen, mem_limit_mb: float | None, timeout_s: float | None) -> None:
        super().__init__(daemon=True)
        self.root = psutil.Process(proc.pid)
        self.proc = proc
        self.mem_limit = mem_limit_mb * MB if mem_limit_mb else None
        self.deadline = time.monotonic() + timeout_s if timeout_s else None
        self.peak = 0
        self.reason = ""
        self._stop_event = threading.Event()

    def tree_rss(self) -> int:
        total = 0
        try:
            procs = [self.root, *self.root.children(recursive=True)]
        except psutil.Error:
            return 0
        for p in procs:
            try:
                total += p.memory_info().rss
            except psutil.Error:
                continue
        return total

    def kill_tree(self) -> None:
        try:
            procs = [*self.root.children(recursive=True), self.root]
        except psutil.Error:
            return
        for p in procs:
            try:
                p.kill()
            except psutil.Error:
                continue

    def run(self) -> None:
        while not self._stop_event.is_set() and self.proc.poll() is None:
            rss = self.tree_rss()
            self.peak = max(self.peak, rss)
            if self.mem_limit and rss > self.mem_limit:
                self.reason = "mem_limit"
            elif self.deadline and time.monotonic() > self.deadline:
                self.reason = "timeout"
            if self.reason:
                self.kill_tree()
                return
            self._stop_event.wait(0.1)

    def stop(self) -> None:
        self._stop_event.set()


def classify_failure(returncode: int, watchdog_reason: str, stderr_tail: str) -> str:
    if watchdog_reason:
        return watchdog_reason
    if "MemoryError" in stderr_tail or "OutOfMemoryError" in stderr_tail or "Java heap space" in stderr_tail:
        return "oom"
    if returncode < 0:
        return "killed"
    return "error"


def run_worker(args: argparse.Namespace, engine: str, slice_path: Path, log_path: Path) -> dict:
    cmd = [sys.executable, str(Path(smoke.__file__).resolve()),
           "--worker-engine", engine, "--worker-slice", str(slice_path),
           "--tasks", *args.tasks, "--repeats", str(args.repeats),
           "--master", args.master, "--driver-memory", args.driver_memory,
           "--shuffle-partitions", str(args.shuffle_partitions)]
    records: list[dict] = []
    current_task = ""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as err:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err, text=True, bufsize=1)
        dog = Watchdog(proc, args.mem_limit_mb, args.timeout_s)
        dog.start()
        assert proc.stdout is not None
        for line in proc.stdout:
            if line.startswith(smoke.RESULT_PREFIX):
                records.append(smoke.json.loads(line[len(smoke.RESULT_PREFIX):]))
            elif line.startswith(smoke.TASK_START_PREFIX):
                current_task = line[len(smoke.TASK_START_PREFIX):].strip()
        returncode = proc.wait()
        dog.stop()
        dog.join()
    stderr_tail = "\n".join(log_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-30:])
    status = "ok" if returncode == 0 and not dog.reason else classify_failure(returncode, dog.reason, stderr_tail)
    error = ""
    if status == "mem_limit":
        error = f"RSS cây tiến trình vượt {args.mem_limit_mb:.0f} MB"
    elif status == "timeout":
        error = f"quá {args.timeout_s:.0f} s"
    elif status != "ok":
        last = [ln.strip() for ln in stderr_tail.splitlines()
                if ln.strip() and " WARN " not in ln and not ln.lstrip().startswith("at ")]
        error = f"rc={returncode}; " + (last[-1][:200] if last else "")
    return {"records": records, "status": status, "error": error, "failed_task": current_task,
            "peak_tree_mb": dog.peak / MB}


# --------------------------------------------------------------------------- #
# Chạy đo
# --------------------------------------------------------------------------- #
def resolve_sizes(raw: list[str], source: Path) -> list[int]:
    import pyarrow.dataset as ds

    total = ds.dataset(source, format="parquet").count_rows() if source.is_file() else sum(
        ds.dataset(sorted(p.glob("*.parquet")), format="parquet").count_rows()
        for p in source.iterdir() if p.is_dir() and p.name.startswith("year_month="))
    sizes = set()
    for item in raw:
        n = total if item == "all" else int(item)
        if n < 1:
            raise ValueError(f"Mốc không hợp lệ: {item}")
        if n > total:
            print(f"Bỏ mốc {n:,}: nguồn chỉ có {total:,} dòng")
            continue
        sizes.add(n)
    return sorted(sizes)


def run_benchmark(args: argparse.Namespace) -> tuple[str, int]:
    sizes = resolve_sizes(args.sizes, args.source)
    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    env = environment_info()
    dataset = smoke.slice_name(args.source).removesuffix("_sample")
    print(f"run_id={run_id}  nguồn={args.source.name}  mốc={[f'{s:,}' for s in sizes]}  cpu={env['cpu_logical']}  "
          f"ram={env['total_ram_gb']} GB  spark={args.master}/{args.driver_memory}  "
          f"mem_limit={args.mem_limit_mb or '-'} MB  timeout={args.timeout_s or '-'} s", flush=True)

    failed_engines: dict[str, str] = {}
    unexpected = 0
    for size in sizes:
        slice_path = smoke.prepare_slice(args.source, size, args.slice_dir)
        for engine in args.engines:
            base = {
                "run_id": run_id, "run_started_utc": started, "engine": engine, "rows": size,
                "slice_file": slice_path.name, "slice_bytes": slice_path.stat().st_size,
                "spark_master": args.master if engine == "spark" else "",
                "spark_driver_memory": args.driver_memory if engine == "spark" else "",
                "spark_shuffle_partitions": args.shuffle_partitions if engine == "spark" else "",
                "dataset": dataset, **env,
            }
            if engine in failed_engines:
                append_csv(args.output, [{**base, "task": "", "repeat": 0, "status": "skipped",
                                          "error": f"đã lỗi ({failed_engines[engine]}) ở mốc nhỏ hơn"}], FIELDS)
                print(f"- {engine:<6} {size:>12,} dòng: bỏ qua", flush=True)
                continue
            print(f"- {engine:<6} {size:>12,} dòng ...", end=" ", flush=True)
            t0 = time.perf_counter()
            log = args.slice_dir / "logs" / f"{run_id}_{engine}_{size}.log"
            out = run_worker(args, engine, slice_path, log)
            rows = [{**base, **rec, "status": "ok", "error": ""} for rec in out["records"]]
            if out["status"] != "ok":
                rows.append({**base, "task": out["failed_task"], "repeat": 0,
                             "peak_rss_mb": round(out["peak_tree_mb"], 1),
                             "status": out["status"], "error": out["error"]})
                failed_engines[engine] = out["status"]
                unexpected += out["status"] == "error"
            append_csv(args.output, rows, FIELDS)
            note = "" if out["status"] == "ok" else f"  [{out['status']} tại {out['failed_task'] or '?'}; log {log.name}]"
            print(f"{time.perf_counter() - t0:.0f}s, đỉnh RSS {out['peak_tree_mb']:.0f} MB{note}", flush=True)
    return run_id, unexpected


# --------------------------------------------------------------------------- #
# Tổng hợp
# --------------------------------------------------------------------------- #
def load_run(csv_path: Path, run_id: str | None):
    import pandas as pd

    df = pd.read_csv(csv_path, dtype={"run_id": str})
    if df.empty:
        raise ValueError(f"{csv_path} không có dữ liệu")
    run_id = run_id or df["run_id"].iloc[-1]
    df = df[df["run_id"] == run_id].copy()
    if df.empty:
        raise ValueError(f"Không có run_id={run_id} trong {csv_path}")
    return run_id, df


def summarise(df):
    """Bảng (rows, task, engine): median wall/RSS, min/max wall, trạng thái."""
    import pandas as pd

    ok = df[(df["status"] == "ok") & (df["task"].isin(smoke.TASKS))]
    agg = ok.groupby(["rows", "task", "engine"]).agg(
        wall_med=("wall_s", "median"), wall_min=("wall_s", "min"), wall_max=("wall_s", "max"),
        rss_med=("peak_rss_mb", "median"), n=("wall_s", "size")).reset_index()
    fails = df[df["status"] != "ok"]
    records = []
    for size in sorted(df["rows"].unique()):
        for engine in smoke.ENGINES:
            fail = fails[(fails["rows"] == size) & (fails["engine"] == engine)]
            for task in smoke.TASKS:
                hit = agg[(agg["rows"] == size) & (agg["task"] == task) & (agg["engine"] == engine)]
                if not hit.empty:
                    rec = hit.iloc[0].to_dict()
                    rec["status"] = "ok"
                elif not fail.empty:
                    f = fail.iloc[0]
                    failed_task = f["task"] if isinstance(f["task"], str) else ""
                    if failed_task in smoke.TASKS and smoke.TASKS.index(task) < smoke.TASKS.index(failed_task):
                        continue  # tác vụ trước điểm lỗi nhưng không có lần đo ok nào
                    rec = {"rows": size, "task": task, "engine": engine, "status": f["status"],
                           "rss_med": f["peak_rss_mb"] if f["status"] != "skipped" else math.nan}
                else:
                    continue
                records.append(rec)
    return pd.DataFrame(records)


def speedups(summary):
    pivot = summary[summary["status"] == "ok"].pivot_table(index=["rows", "task"], columns="engine",
                                                           values="wall_med")
    if not {"pandas", "spark"} <= set(pivot.columns):
        return pivot.iloc[0:0]
    pivot = pivot.dropna()
    pivot["speedup"] = pivot["pandas"] / pivot["spark"]
    return pivot.reset_index()


def checksum_problems(df) -> list[str]:
    ok = df[(df["status"] == "ok") & (df["task"].isin(smoke.TASKS))]
    problems = []
    for (size, task), grp in ok.groupby(["rows", "task"]):
        if grp["engine"].nunique() < 2:
            continue
        sums = grp["checksum"].astype(float)
        rel = (sums.max() - sums.min()) / (sums.abs().max() or 1.0)
        if grp["result_rows"].nunique() != 1 or rel > smoke.CHECKSUM_REL_TOL:
            problems.append(f"{task} @ {size:,}: result_rows={sorted(grp['result_rows'].unique())}, rel_diff={rel:.2e}")
    return problems


def fmt_rows(n: int) -> str:
    return f"{n / 1e6:.2f}M".replace(".00M", "M") if n >= 1_000_000 else f"{n / 1e3:.0f}K"


def write_summary_md(df, summary, run_id: str, path: Path, figures: list[Path]) -> None:
    env = df.iloc[0]
    startup = df[(df["task"] == "spark_startup") & (df["status"] == "ok")]["wall_s"]
    sp = speedups(summary)
    lines = [
        f"# [D3] Kết quả benchmark Pandas vs PySpark ({env['dataset']})",
        "",
        f"Sinh tự động bởi `03_benchmarks_similarity/benchmark_scaling_pandas_vs_spark.py`, run `{run_id}` "
        f"({env['run_started_utc']}).",
        "",
        f"- Máy: {env['machine']}, {int(env['cpu_logical'])} CPU logic, {env['total_ram_gb']} GB RAM, "
        f"Python {env['python']}, pandas {env['pandas']}, pyarrow {env['pyarrow']}, PySpark {env['pyspark']}.",
        f"- Spark: `{next((m for m in df['spark_master'].dropna() if m), '-')}`, driver "
        f"`{next((m for m in df['spark_driver_memory'].dropna() if m), '-')}`, "
        f"khởi động SparkSession median {startup.median():.2f} s (không tính vào thời gian tác vụ).",
        f"- Mỗi ô là median của {int(summary['n'].max()) if 'n' in summary and summary['n'].notna().any() else '-'} "
        "lần đo sau 1 lần khởi động. RAM là đỉnh RSS cả cây tiến trình (Python + JVM với Spark).",
        "",
        "| Số dòng | Tác vụ | Pandas (s) | PySpark (s) | Speedup | Pandas RAM (MB) | PySpark RAM (MB) |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for size in sorted(summary["rows"].unique()):
        for task in smoke.TASKS:
            cells = []
            for engine in smoke.ENGINES:
                hit = summary[(summary["rows"] == size) & (summary["task"] == task) & (summary["engine"] == engine)]
                if hit.empty:
                    cells.append(("—", "—"))
                elif hit.iloc[0]["status"] == "ok":
                    cells.append((f"{hit.iloc[0]['wall_med']:.3f}", f"{hit.iloc[0]['rss_med']:.0f}"))
                else:
                    st = hit.iloc[0]["status"]
                    rss = hit.iloc[0]["rss_med"]
                    cells.append((st, f"> {rss:.0f}" if st in FAIL_STATUSES and rss == rss else "—"))
            s = sp[(sp["rows"] == size) & (sp["task"] == task)] if not sp.empty else sp
            speed = f"{s.iloc[0]['speedup']:.2f}" if not s.empty else "—"
            lines.append(f"| {size:,} | {TASK_LABELS[task]} | {cells[0][0]} | {cells[1][0]} | {speed} | "
                         f"{cells[0][1]} | {cells[1][1]} |")
    lines += [
        "",
        "Speedup = Pandas / PySpark, > 1 nghĩa là PySpark nhanh hơn. Trạng thái khác `ok`: `mem_limit` vượt ngân sách RAM "
        "đặt bằng `--mem-limit-mb`, `killed` bị hệ điều hành dừng (OOM killer), `oom` MemoryError/OutOfMemoryError, "
        "`timeout` quá `--timeout-s`, `skipped` không chạy vì đã lỗi ở mốc nhỏ hơn.",
        "",
        "Biểu đồ:",
        "",
        *[f"- `{p.relative_to(path.parent)}`" for p in figures],
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Biểu đồ
# --------------------------------------------------------------------------- #
def style_axes(ax) -> None:
    ax.set_facecolor("white")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=MUTED, labelcolor=INK_2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def plot_metric(summary, metric: str, ylabel: str, title: str, path: Path, mem_limit_mb: float | None = None) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, len(smoke.TASKS), figsize=(12, 3.8), dpi=150)
    fig.patch.set_facecolor("white")
    x_max = max(summary["rows"]) / 1e6
    for ax, task in zip(axes, smoke.TASKS):
        style_axes(ax)
        tops = [mem_limit_mb] if (mem_limit_mb and metric == "rss_med") else []
        failures = []
        for engine in smoke.ENGINES:
            part = summary[(summary["task"] == task) & (summary["engine"] == engine)].sort_values("rows")
            okp = part[part["status"] == "ok"]
            color = ENGINE_COLORS[engine]
            x = okp["rows"] / 1e6
            ax.plot(x, okp[metric], color=color, linewidth=2, marker="o", markersize=5,
                    markeredgecolor="white", markeredgewidth=1.2, label=ENGINE_LABELS[engine], zorder=3)
            if metric == "wall_med" and not okp.empty:
                ax.vlines(x, okp["wall_min"], okp["wall_max"], color=color, linewidth=1, alpha=0.6, zorder=2)
                tops.append(okp["wall_max"].max())
            if not okp.empty:
                tops.append(okp[metric].max())
                last = okp.iloc[-1]
                ax.annotate(ENGINE_LABELS[engine], (last["rows"] / 1e6, last[metric]), xytext=(6, 0),
                            textcoords="offset points", va="center", fontsize=8, color=INK_2)
            for _, f in part[part["status"].isin(FAIL_STATUSES)].iterrows():
                has_rss = metric == "rss_med" and f["rss_med"] == f["rss_med"]
                if has_rss:
                    tops.append(f["rss_med"])
                failures.append((f["rows"] / 1e6, f["rss_med"] if has_rss else None, f["status"], color))
        top = max(tops) * 1.3 if tops else 1.0
        ax.set_ylim(0, top)
        for xv, yv, status, color in failures:
            label = status if yv is not None else f"{status}\n(không có số đo)"
            yv = top * 0.9 if yv is None else yv
            ax.plot(xv, yv, marker="X", markersize=9, color=color, markeredgecolor=INK,
                    markeredgewidth=0.8, zorder=4)
            ax.annotate(label, (xv, yv), xytext=(-8, 0), textcoords="offset points",
                        ha="right", va="center", fontsize=8, color=INK)
        if mem_limit_mb and metric == "rss_med":
            ax.axhline(mem_limit_mb, color=MUTED, linewidth=1, linestyle="--", zorder=1)
            ax.annotate(f"giới hạn {mem_limit_mb:.0f} MB", (0, mem_limit_mb), xycoords=("axes fraction", "data"),
                        xytext=(4, 4), textcoords="offset points", ha="left", fontsize=8, color=MUTED)
        ax.set_title(TASK_LABELS[task], fontsize=10, color=INK, loc="left", pad=10)
        ax.set_xlabel("Số dòng (triệu)", fontsize=9, color=INK_2)
        ax.set_xlim(left=0, right=x_max * 1.18)
    axes[0].set_ylabel(ylabel, fontsize=9, color=INK_2)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=2, frameon=False, fontsize=9, labelcolor=INK_2)
    fig.suptitle(title, x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(path, facecolor="white")
    plt.close(fig)


def plot_speedup(summary, path: Path) -> bool:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sp = speedups(summary)
    if sp.empty:
        return False
    fig, ax = plt.subplots(figsize=(7, 4), dpi=150)
    fig.patch.set_facecolor("white")
    style_axes(ax)
    for task in smoke.TASKS:
        part = sp[sp["task"] == task].sort_values("rows")
        if part.empty:
            continue
        color = TASK_COLORS[task]
        ax.plot(part["rows"] / 1e6, part["speedup"], color=color, linewidth=2, marker="o", markersize=5,
                markeredgecolor="white", markeredgewidth=1.2, label=TASK_LABELS[task], zorder=3)
        last = part.iloc[-1]
        ax.annotate(TASK_LABELS[task], (last["rows"] / 1e6, last["speedup"]), xytext=(6, 0),
                    textcoords="offset points", va="center", fontsize=8, color=INK_2)
    ax.axhline(1, color=MUTED, linewidth=1, linestyle="--")
    ax.annotate("hòa (speedup = 1)", (0, 1), xycoords=("axes fraction", "data"), xytext=(2, 3),
                textcoords="offset points", fontsize=8, color=MUTED)
    ax.set_ylim(bottom=0, top=max(1.2, sp["speedup"].max() * 1.15))
    ax.set_xlim(left=0, right=sp["rows"].max() / 1e6 * 1.35)
    ax.set_xlabel("Số dòng (triệu)", fontsize=9, color=INK_2)
    ax.set_ylabel("Speedup = Pandas / PySpark", fontsize=9, color=INK_2)
    ax.legend(frameon=False, fontsize=8, labelcolor=INK_2, loc="upper left")
    fig.suptitle("Hệ số tăng tốc của PySpark so với Pandas", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return True


def report(args: argparse.Namespace, run_id: str | None) -> list[str]:
    run_id, df = load_run(args.output, run_id)
    summary = summarise(df)
    fig_dir = args.output.parent / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)
    dataset = df["dataset"].iloc[0]
    figures = [fig_dir / f"benchmark_{dataset}_wall_time.png", fig_dir / f"benchmark_{dataset}_peak_ram.png"]
    limit = args.mem_limit_mb
    plot_metric(summary, "wall_med", "Thời gian (s), median", "Thời gian thực thi theo số dòng", figures[0])
    plot_metric(summary, "rss_med", "Đỉnh RAM (MB), median", "Đỉnh RAM cây tiến trình theo số dòng",
                figures[1], limit)
    sp_path = fig_dir / f"benchmark_{dataset}_speedup.png"
    if plot_speedup(summary, sp_path):
        figures.append(sp_path)
    md = args.output.parent / f"benchmark_summary_{dataset}.md"
    write_summary_md(df, summary, run_id, md, figures)
    print(f"\nrun_id={run_id}: {md.name} + {len(figures)} biểu đồ trong {fig_dir}")
    print("\n".join(ln for ln in md.read_text(encoding="utf-8").splitlines() if ln.startswith("|")))
    return checksum_problems(df)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help="File hoặc thư mục Parquet long-format (mặc định: PEMS-BAY của [K2])")
    parser.add_argument("--slice-dir", type=Path, default=PROJECT / "data/interim/bench")
    parser.add_argument("--output", type=Path, default=HERE / "benchmark_results.csv")
    parser.add_argument("--sizes", nargs="+", default=list(DEFAULT_SIZES), help='Số dòng hoặc "all"')
    parser.add_argument("--engines", nargs="+", choices=smoke.ENGINES, default=list(smoke.ENGINES))
    parser.add_argument("--tasks", nargs="+", choices=smoke.TASKS, default=list(smoke.TASKS))
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[*]"))
    parser.add_argument("--driver-memory", default=os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--mem-limit-mb", type=float, default=None,
                        help="Ngân sách RAM cho cả cây tiến trình mỗi lần chạy; vượt thì dừng, ghi mem_limit")
    parser.add_argument("--timeout-s", type=float, default=1800, help="Giới hạn thời gian mỗi cặp (engine, mốc)")
    parser.add_argument("--plot-only", action="store_true", help="Chỉ vẽ lại biểu đồ và bảng từ CSV")
    parser.add_argument("--run-id", help="run_id cần vẽ khi --plot-only (mặc định: lần chạy cuối)")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats phải dương")

    unexpected = 0
    run_id = args.run_id
    if not args.plot_only:
        if not args.source.exists():
            parser.error(f"Không thấy {args.source}. Chạy 02_pyspark_gigo/csv_to_partitioned_parquet.py trước "
                         "hoặc truyền --source.")
        run_id, unexpected = run_benchmark(args)
    problems = report(args, run_id)
    if problems:
        print("\nChecksum Pandas/PySpark lệch:\n" + "\n".join(problems))
    if unexpected:
        print(f"\n{unexpected} lần chạy lỗi ngoài dự kiến (status=error), xem {args.slice_dir / 'logs'}")
    return 1 if problems or unexpected else 0


if __name__ == "__main__":
    sys.exit(main())
