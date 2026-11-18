"""Fetch hourly historical Hàng Xanh weather from Open-Meteo (Week 4, T2)."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE, LONGITUDE = 10.8012, 106.7114
TIMEZONE = "Asia/Ho_Chi_Minh"
VARS = ["precipitation", "temperature_2m", "relative_humidity_2m", "wind_speed_10m"]


def fetch(start: date, end: date) -> pd.DataFrame:
    params = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "hourly": ",".join(VARS), "timezone": TIMEZONE,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
    }
    response = requests.get(URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if "hourly" not in payload:
        raise ValueError(f"Unexpected Open-Meteo response: {payload}")
    df = pd.DataFrame(payload["hourly"]).rename(columns={"time": "timestamp"})
    # API returns local wall time when a timezone is requested. Attach the
    # offset so exported ISO-8601 timestamps remain unambiguous downstream.
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(TIMEZONE)
    df["timezone"] = payload.get("timezone", TIMEZONE)
    df["latitude"] = payload.get("latitude")
    df["longitude"] = payload.get("longitude")
    df["requested_latitude"] = LATITUDE
    df["requested_longitude"] = LONGITUDE
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # Open-Meteo's archive has a short publication delay; use a 90-day window
    # ending five days ago to provide both dry and rainy conditions.
    parser.add_argument("--start", type=date.fromisoformat, default=date.today() - timedelta(days=94))
    parser.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=5))
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("open_meteo_hcm_sample.csv"))
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must be on or before --end")
    df = fetch(args.start, args.end)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} hourly rows to {args.output}")
    daily_rain = df.assign(date=df["timestamp"].dt.date).groupby("date")["precipitation"].sum()
    print(
        f"Period: {df['timestamp'].min()} -> {df['timestamp'].max()} | "
        f"rain hours: {(df['precipitation'] > 0).sum()} | "
        f"wet/dry days: {(daily_rain > 0).sum()}/{(daily_rain == 0).sum()} | "
        f"max hourly/daily precipitation: {df['precipitation'].max():.1f}/{daily_rain.max():.1f} mm"
    )
    print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()
