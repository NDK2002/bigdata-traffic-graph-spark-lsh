"""Download checksummed METR-LA / PEMS-BAY inputs and DCRNN graph metadata."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import requests


ZENODO_API = "https://zenodo.org/api/records/5146275"
DCRNN_COMMIT = "602afd9d767d3aa1c9b3eac51710d6aeee12c227"
DCRNN_RAW = f"https://raw.githubusercontent.com/liyaguang/DCRNN/{DCRNN_COMMIT}/data/sensor_graph"
ZENODO_FILES = {"METR-LA.csv", "PEMS-BAY.csv", "PEMS-BAY-META.csv", "adj_mx_bay.pkl"}


def md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, destination: Path, expected_size: int | None = None,
             expected_md5: str | None = None, force: bool = False) -> None:
    if destination.exists() and not force:
        if expected_size is not None and destination.stat().st_size != expected_size:
            raise ValueError(f"Existing file has wrong size: {destination}")
        if expected_md5 and md5_file(destination) != expected_md5:
            raise ValueError(f"Existing file checksum mismatch: {destination}")
        print(f"Verified existing {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    temporary.unlink(missing_ok=True)
    with requests.get(url, stream=True, timeout=(30, 180)) as response:
        response.raise_for_status()
        with temporary.open("wb") as output:
            for chunk in response.iter_content(1024 * 1024):
                if chunk:
                    output.write(chunk)
    if expected_size is not None and temporary.stat().st_size != expected_size:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"Downloaded size mismatch for {destination.name}")
    if expected_md5 and md5_file(temporary) != expected_md5:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"Downloaded checksum mismatch for {destination.name}")
    temporary.replace(destination)
    print(f"Downloaded and verified {destination}")


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=project / "data/raw")
    parser.add_argument("--ingestion-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--force", action="store_true", help="Download again and replace existing files")
    args = parser.parse_args()

    record = requests.get(ZENODO_API, timeout=30)
    record.raise_for_status()
    files = {item["key"]: item for item in record.json()["files"]}
    missing = ZENODO_FILES - files.keys()
    if missing:
        raise RuntimeError(f"Zenodo record no longer contains expected files: {sorted(missing)}")
    for name in sorted(ZENODO_FILES):
        item = files[name]
        download(item["links"]["self"], args.raw_dir / name,
                 expected_size=item["size"], expected_md5=item["checksum"].split(":", 1)[-1],
                 force=args.force)

    # Official DCRNN sensor coordinates and METR-LA graph adjacency.
    download(f"{DCRNN_RAW}/graph_sensor_locations.csv",
             args.ingestion_dir / "graph_sensor_locations.csv", force=args.force)
    download(f"{DCRNN_RAW}/adj_mx.pkl", args.raw_dir / "adj_mx.pkl", force=args.force)


if __name__ == "__main__":
    main()
