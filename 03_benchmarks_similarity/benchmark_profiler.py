"""[D1] Khung đo tài nguyên dùng chung cho benchmark Pandas vs PySpark (Nhóm 11).

Đo trong một khối lệnh:
    wall_s            thời gian thực (time.perf_counter)
    peak_rss_mb       đỉnh RSS của cả cây tiến trình: Python + tiến trình con (JVM của Spark)
    peak_python_mb    đỉnh RSS riêng tiến trình Python
    peak_children_mb  đỉnh RSS các tiến trình con (0 với Pandas)
    baseline_rss_mb   RSS cây tiến trình ngay trước khi đo
    delta_rss_mb      peak_rss_mb - baseline_rss_mb

RSS được lấy mẫu bằng psutil trong luồng nền (mặc định 20 ms). Đỉnh ngắn hơn chu kỳ lấy mẫu có thể bị bỏ sót.
Peak RSS của PySpark gồm cả JVM driver nên luôn có phần nền vài trăm MB, không so trực tiếp với heap Pandas.

Ví dụ:
    from benchmark_profiler import measure, append_csv, environment_info
    with measure() as m:
        run_task()
    append_csv("results.csv", [{**environment_info(), **m.as_dict(), "engine": "pandas"}])
"""
from __future__ import annotations

import csv
import gc
import os
import platform
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Iterator

import psutil

MB = 1024 * 1024


@dataclass
class Measurement:
    wall_s: float = 0.0
    peak_rss_mb: float = 0.0
    peak_python_mb: float = 0.0
    peak_children_mb: float = 0.0
    baseline_rss_mb: float = 0.0
    delta_rss_mb: float = 0.0
    samples: int = 0

    def as_dict(self) -> dict:
        return {k: round(v, 4) if isinstance(v, float) else v for k, v in asdict(self).items()}


def tree_rss(proc: psutil.Process) -> tuple[int, int]:
    """Trả về (RSS tiến trình hiện tại, tổng RSS các tiến trình con) tính bằng byte."""
    own = proc.memory_info().rss
    children = 0
    for child in proc.children(recursive=True):
        try:
            children += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return own, children


class _Sampler(threading.Thread):
    def __init__(self, proc: psutil.Process, interval: float) -> None:
        super().__init__(daemon=True)
        self.proc = proc
        self.interval = interval
        self.peak_total = self.peak_own = self.peak_children = 0
        self.samples = 0
        self._stop_event = threading.Event()

    def sample(self) -> None:
        own, children = tree_rss(self.proc)
        self.peak_own = max(self.peak_own, own)
        self.peak_children = max(self.peak_children, children)
        self.peak_total = max(self.peak_total, own + children)
        self.samples += 1

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.sample()
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()


@contextmanager
def measure(interval_s: float = 0.02, collect_garbage: bool = True) -> Iterator[Measurement]:
    """Đo wall-clock và đỉnh RSS của cây tiến trình trong khối `with`."""
    if collect_garbage:
        gc.collect()
    proc = psutil.Process(os.getpid())
    own, children = tree_rss(proc)
    result = Measurement(baseline_rss_mb=(own + children) / MB)
    sampler = _Sampler(proc, interval_s)
    sampler.sample()
    sampler.start()
    start = time.perf_counter()
    try:
        yield result
    finally:
        result.wall_s = time.perf_counter() - start
        sampler.stop()
        sampler.join()
        sampler.sample()
        result.peak_rss_mb = sampler.peak_total / MB
        result.peak_python_mb = sampler.peak_own / MB
        result.peak_children_mb = sampler.peak_children / MB
        result.delta_rss_mb = result.peak_rss_mb - result.baseline_rss_mb
        result.samples = sampler.samples


def _java_version() -> str:
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home, "bin", "java")) if java_home else shutil.which("java")
    if not java:
        return ""
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return ""
    lines = [ln for ln in (out.stderr or out.stdout).splitlines() if "version" in ln]
    return lines[0].strip() if lines else ""


def _package_version(name: str) -> str:
    try:
        from importlib.metadata import version
        return version(name)
    except Exception:  # noqa: BLE001 — gói không cài thì để trống
        return ""


def environment_info() -> dict:
    """Thông tin máy và phiên bản thư viện, ghi kèm mỗi dòng kết quả để so sánh giữa các máy."""
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_logical": psutil.cpu_count(logical=True),
        "cpu_physical": psutil.cpu_count(logical=False),
        "total_ram_gb": round(psutil.virtual_memory().total / 1024 ** 3, 2),
        "python": platform.python_version(),
        "pandas": _package_version("pandas"),
        "pyarrow": _package_version("pyarrow"),
        "pyspark": _package_version("pyspark"),
        "java": _java_version(),
    }


def append_csv(path: str | Path, rows: Iterable[dict], fieldnames: list[str] | None = None) -> Path:
    """Ghi nối các dòng kết quả. Tạo header khi file mới, báo lỗi nếu cột lệch header cũ."""
    rows = list(rows)
    if not rows:
        return Path(path)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = fieldnames or list(rows[0].keys())
    if path.exists() and path.stat().st_size > 0:
        with path.open(newline="", encoding="utf-8") as stream:
            existing = next(csv.reader(stream), [])
        if existing != fields:
            raise ValueError(f"Cột không khớp header hiện có của {path}: {existing} != {fields}")
        mode = "a"
    else:
        mode = "w"
    with path.open(mode, newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="raise")
        if mode == "w":
            writer.writeheader()
        writer.writerows(rows)
    return path


if __name__ == "__main__":
    with measure() as m:
        block = bytearray(200 * MB)
        time.sleep(0.2)
        del block
    print(m.as_dict())
