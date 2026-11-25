"""[D2] Bản đồ GIS kiểm chứng đồ thị Hàng Xanh và lớp cảm biến METR-LA (Nhóm 11).

Đầu vào (01_data_ingestion/):
    hang_xanh_nodes.csv, hang_xanh_edges.csv      ([H3])
    graph_sensor_locations.csv                    ([T1])
    metr-la_sensor_audit.csv                      ([T3], tùy chọn: tô màu theo tỷ lệ zero)

Đầu ra (04_hcm_dashboard/):
    hang_xanh_gis_check.html     nút giao, cạnh có hướng, vòng bán kính trích xuất
    metr_la_sensors_map.html     207 cảm biến METR-LA, tô màu theo tỷ lệ zero sentinel
    gis_inspection_notes.md      kết quả kiểm tra tọa độ, chiều đường, độ dài cạnh

Chạy:
    python 04_hcm_dashboard/build_gis_inspection_map.py
    python 04_hcm_dashboard/build_gis_inspection_map.py --strict   # mã thoát 1 nếu có kiểm tra FAIL
"""
from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path

import folium
import pandas as pd
from folium.plugins import Fullscreen, MeasureControl, PolyLineTextPath

HANG_XANH_CENTER = (10.8012, 106.7114)
EXTRACT_RADIUS_M = 1000.0
MAIN_ROADS = {
    "Xô Viết Nghệ Tĩnh": "#d62728",
    "Điện Biên Phủ": "#1f77b4",
    "Đinh Bộ Lĩnh": "#2ca02c",
    "Bạch Đằng": "#9467bd",
    "Nguyễn Gia Trí": "#ff7f0e",
    "Ung Văn Khiêm": "#8c564b",
}
OTHER_ROAD_COLOR = "#e377c2"
LA_BBOX = (33.6, 34.5, -118.8, -117.7)
ZERO_BUCKETS = [
    (0.0, 7.0, "#2ca02c", "zero < 7% (mức nền)"),
    (7.0, 10.0, "#bcbd22", "zero 7–10%"),
    (10.0, 15.0, "#ff7f0e", "zero 10–15%"),
    (15.0, 101.0, "#d62728", "zero ≥ 15%"),
]
LENGTH_RATIO_MIN = 0.9


@dataclass
class Check:
    name: str
    ok: bool
    detail: str
    warn_only: bool = False

    @property
    def status(self) -> str:
        if self.ok:
            return "PASS"
        return "WARN" if self.warn_only else "FAIL"


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def road_group(name: str) -> str:
    for road in MAIN_ROADS:
        if road in name:
            return road
    return name


def legend_label(color: str, text: str) -> str:
    return f'<span style="color:{color};font-size:16px">■</span> {text}'


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


# --------------------------------------------------------------------------- #
# Hàng Xanh
# --------------------------------------------------------------------------- #
def check_hang_xanh(nodes: pd.DataFrame, edges: pd.DataFrame) -> tuple[list[Check], pd.DataFrame]:
    checks: list[Check] = []
    pos = nodes.set_index("node_id")[["lat", "lon"]]

    n = len(nodes)
    checks.append(Check("Số nút trong ngưỡng 15–25", 15 <= n <= 25, f"{n} nút"))
    checks.append(Check("node_id / node_code duy nhất",
                        nodes["node_id"].is_unique and nodes["node_code"].is_unique, ""))

    dist = nodes.apply(lambda r: haversine_m(r.lat, r.lon, *HANG_XANH_CENTER), axis=1)
    diff = (dist - nodes["dist_center_m"]).abs()
    checks.append(Check("dist_center_m khớp khoảng cách haversine tới tâm (sai lệch ≤ 25 m)",
                        bool((diff <= 25).all()), f"sai lệch lớn nhất {diff.max():.1f} m"))
    outside = nodes.loc[dist > EXTRACT_RADIUS_M, "node_code"].tolist()
    checks.append(Check(f"Nút nằm trong bán kính trích xuất {EXTRACT_RADIUS_M:.0f} m",
                        not outside,
                        f"ngoài bán kính: {', '.join(f'{c} ({d:.0f} m)' for c, d in zip(outside, dist[dist > EXTRACT_RADIUS_M]))}"
                        if outside else "tất cả nằm trong", warn_only=True))

    utm_zero = int(((nodes["utm_x"] == 0) & (nodes["utm_y"] == 0)).sum())
    checks.append(Check("utm_x/utm_y có giá trị (UTM 48N)", utm_zero == 0,
                        f"{utm_zero}/{n} nút có utm_x = utm_y = 0" if utm_zero else ""))

    missing = sorted(set(edges["u"]).union(edges["v"]) - set(pos.index))
    checks.append(Check("Đầu mút cạnh tồn tại trong bảng nút", not missing,
                        f"thiếu {missing}" if missing else f"{len(edges)} cạnh"))
    self_loops = int((edges["u"] == edges["v"]).sum())
    checks.append(Check("Không có self-loop", self_loops == 0, f"{self_loops} self-loop"))
    dup = int(edges.duplicated(["u", "v"]).sum())
    checks.append(Check("Không trùng cạnh (u, v)", dup == 0, f"{dup} cạnh trùng"))

    out_deg = edges.groupby("u").size().reindex(nodes["node_id"], fill_value=0).to_numpy()
    in_deg = edges.groupby("v").size().reindex(nodes["node_id"], fill_value=0).to_numpy()
    bad_deg = nodes.loc[(out_deg != nodes["out_degree"].to_numpy()) | (in_deg != nodes["in_degree"].to_numpy()), "node_code"]
    checks.append(Check("in_degree/out_degree khớp bảng cạnh", bad_deg.empty,
                        f"lệch tại {', '.join(bad_deg)}" if not bad_deg.empty else ""))
    isolated = nodes.loc[(out_deg + in_deg) == 0, "node_code"].tolist()
    checks.append(Check("Không có nút cô lập", not isolated, ", ".join(isolated)))
    sources = nodes.loc[(in_deg == 0) & (out_deg > 0), "node_code"].tolist()
    sinks = nodes.loc[(out_deg == 0) & (in_deg > 0), "node_code"].tolist()
    checks.append(Check("Nút chỉ đi ra / chỉ đi vào (biên đồ thị)", True,
                        f"chỉ ra: {', '.join(sources) or '—'} · chỉ vào: {', '.join(sinks) or '—'}"))

    pairs = set(zip(edges["u"], edges["v"]))
    edges = edges.copy()
    edges["has_reverse"] = [(v, u) in pairs for u, v in zip(edges["u"], edges["v"])]
    flag_mismatch = edges.loc[edges["has_reverse"] != edges["bidirectional_pair"].astype(bool), "edge_id"].tolist()
    checks.append(Check("Cờ bidirectional_pair khớp sự tồn tại cạnh ngược", not flag_mismatch,
                        ", ".join(flag_mismatch)))
    twoway_no_reverse = edges.loc[~edges["oneway"].astype(bool) & ~edges["has_reverse"], "edge_id"].tolist()
    checks.append(Check("Cạnh oneway=False có cạnh ngược", not twoway_no_reverse, ", ".join(twoway_no_reverse)))
    by_pair = edges.set_index(["u", "v"])["oneway"].astype(bool)
    oneway_conflict = sorted({tuple(sorted((u, v))) for (u, v), ow in by_pair.items()
                              if (v, u) in by_pair.index and by_pair[(v, u)] != ow})
    checks.append(Check("Hai chiều của cùng cặp nút thống nhất cờ oneway", not oneway_conflict,
                        ", ".join(f"HX_{a:02d}↔HX_{b:02d}" for a, b in oneway_conflict)))

    edges["straight_m"] = [haversine_m(*pos.loc[u], *pos.loc[v]) for u, v in zip(edges["u"], edges["v"])]
    edges["length_ratio"] = edges["length"] / edges["straight_m"]
    short = edges.loc[edges["length_ratio"] < LENGTH_RATIO_MIN]
    checks.append(Check(f"Độ dài cạnh ≥ {LENGTH_RATIO_MIN:.0%} khoảng cách đường chim bay", short.empty,
                        ", ".join(f"{r.edge_id} ({r.length:.0f}/{r.straight_m:.0f} m)" for r in short.itertuples())
                        or f"tỷ lệ nhỏ nhất {edges['length_ratio'].min():.3f}"))
    return checks, edges


def build_hang_xanh_map(nodes: pd.DataFrame, edges: pd.DataFrame, out: Path) -> None:
    fmap = folium.Map(location=HANG_XANH_CENTER, zoom_start=16, tiles=None, control_scale=True)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap").add_to(fmap)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Light Gray Canvas", name="Nền xám nhạt (Esri)", show=False).add_to(fmap)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Imagery", name="Ảnh vệ tinh (Esri)", show=False).add_to(fmap)

    ref = folium.FeatureGroup(name="Tâm Hàng Xanh + bán kính 1000 m")
    folium.Circle(HANG_XANH_CENTER, radius=EXTRACT_RADIUS_M, color="#555", weight=1,
                  dash_array="6,6", fill=False).add_to(ref)
    folium.Marker(HANG_XANH_CENTER, tooltip=f"Tâm trích xuất {HANG_XANH_CENTER}",
                  icon=folium.Icon(color="gray", icon="screenshot", prefix="glyphicon")).add_to(ref)
    ref.add_to(fmap)

    pos = nodes.set_index("node_id")
    edges = edges.assign(group=edges["name"].map(road_group))
    drawn: set[tuple[int, int]] = set()
    groups: dict[str, folium.FeatureGroup] = {}
    for group in sorted(edges["group"].unique(), key=lambda g: (g not in MAIN_ROADS, g)):
        color = MAIN_ROADS.get(group, OTHER_ROAD_COLOR)
        groups[group] = folium.FeatureGroup(name=legend_label(color, group)).add_to(fmap)

    for e in edges.itertuples():
        key = tuple(sorted((e.u, e.v)))
        if e.has_reverse and key in drawn:
            continue
        drawn.add(key)
        color = MAIN_ROADS.get(e.group, OTHER_ROAD_COLOR)
        a, b = pos.loc[e.u], pos.loc[e.v]
        coords = [(a.lat, a.lon), (b.lat, b.lon)]
        if e.has_reverse:
            back = edges[(edges["u"] == e.v) & (edges["v"] == e.u)].iloc[0]
            tip = (f"<b>{e.u_code} ↔ {e.v_code}</b><br>{e.name}<br>"
                   f"{e.edge_id}: {e.length:.0f} m · {e.lanes} làn · {e.maxspeed} km/h<br>"
                   f"{back.edge_id}: {back.length:.0f} m · {back.lanes} làn · {back.maxspeed} km/h")
            folium.PolyLine(coords, color=color, weight=6, opacity=0.85, tooltip=tip).add_to(groups[e.group])
        else:
            tip = (f"<b>{e.u_code} → {e.v_code}</b> (một chiều)<br>{e.name}<br>"
                   f"{e.edge_id}: {e.length:.0f} m · {e.lanes} làn · {e.maxspeed} km/h")
            line = folium.PolyLine(coords, color=color, weight=5, opacity=0.85, dash_array="10,6", tooltip=tip)
            line.add_to(groups[e.group])
            PolyLineTextPath(line, "  ►  ", repeat=True, offset=6,
                             attributes={"fill": color, "font-size": "14"}).add_to(groups[e.group])

    node_layer = folium.FeatureGroup(name="Nút giao HX_01–HX_20").add_to(fmap)
    for r in nodes.itertuples():
        popup = (f"<b>{r.node_code}</b> (node_id {r.node_id})<br>{r.roads}<br>"
                 f"lat {r.lat:.6f}, lon {r.lon:.6f}<br>"
                 f"in/out: {r.in_degree}/{r.out_degree} · cách tâm {r.dist_center_m:.0f} m")
        folium.CircleMarker((r.lat, r.lon), radius=7, color="#111", weight=2, fill=True,
                            fill_color="#ffd700", fill_opacity=1.0,
                            tooltip=r.node_code, popup=folium.Popup(popup, max_width=320)).add_to(node_layer)
        folium.Marker((r.lat, r.lon), icon=folium.DivIcon(
            icon_size=(48, 14), icon_anchor=(-8, 18),
            html=f'<div style="font:bold 11px sans-serif;color:#111;text-shadow:0 0 3px #fff">{r.node_code}</div>'
        )).add_to(node_layer)

    folium.LayerControl(collapsed=False).add_to(fmap)
    MeasureControl(primary_length_unit="meters").add_to(fmap)
    Fullscreen().add_to(fmap)
    fmap.save(out)


# --------------------------------------------------------------------------- #
# METR-LA
# --------------------------------------------------------------------------- #
def check_metr_la(sensors: pd.DataFrame, audit: pd.DataFrame | None) -> list[Check]:
    checks = [
        Check("Đủ 207 cảm biến", len(sensors) == 207, f"{len(sensors)} dòng"),
        Check("sensor_id duy nhất", sensors["sensor_id"].is_unique, ""),
        Check("Không trùng tọa độ", not sensors.duplicated(["latitude", "longitude"]).any(),
              f"{int(sensors.duplicated(['latitude', 'longitude']).sum())} điểm trùng"),
    ]
    lat0, lat1, lon0, lon1 = LA_BBOX
    out = sensors[~sensors["latitude"].between(lat0, lat1) | ~sensors["longitude"].between(lon0, lon1)]
    checks.append(Check(f"Tọa độ nằm trong vùng Los Angeles {LA_BBOX}", out.empty,
                        ", ".join(out["sensor_id"].astype(str)) or
                        f"lat {sensors.latitude.min():.4f}–{sensors.latitude.max():.4f}, "
                        f"lon {sensors.longitude.min():.4f}–{sensors.longitude.max():.4f}"))
    if audit is not None:
        diff = set(sensors["sensor_id"].astype(str)) ^ set(audit["sensor_id"].astype(str))
        checks.append(Check("sensor_id khớp metr-la_sensor_audit.csv", not diff,
                            f"lệch {sorted(diff)[:10]}" if diff else "207/207"))
    return checks


def build_metr_la_map(sensors: pd.DataFrame, audit: pd.DataFrame | None, out: Path) -> None:
    center = (sensors["latitude"].mean(), sensors["longitude"].mean())
    fmap = folium.Map(location=center, zoom_start=11, tiles=None, control_scale=True)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Esri World Light Gray Canvas", name="Nền xám nhạt (Esri)").add_to(fmap)
    folium.TileLayer("OpenStreetMap", name="OpenStreetMap", show=False).add_to(fmap)
    fmap.fit_bounds([[sensors["latitude"].min(), sensors["longitude"].min()],
                     [sensors["latitude"].max(), sensors["longitude"].max()]])

    data = sensors.assign(sensor_id=sensors["sensor_id"].astype(str))
    if audit is not None:
        cols = ["sensor_id", "zero_pct", "iqr_outlier_count", "observed_speed_count", "q1", "q3"]
        data = data.merge(audit.assign(sensor_id=audit["sensor_id"].astype(str))[cols], on="sensor_id", how="left")
        layers = []
        for lo, hi, color, label in ZERO_BUCKETS:
            part = data[(data["zero_pct"] >= lo) & (data["zero_pct"] < hi)]
            layers.append((part, color, f"{label} ({len(part)} cảm biến)"))
    else:
        layers = [(data, "#1f77b4", f"Cảm biến METR-LA ({len(data)})")]

    for part, color, label in layers:
        group = folium.FeatureGroup(name=legend_label(color, label)).add_to(fmap)
        for r in part.itertuples():
            popup = f"<b>sensor {r.sensor_id}</b><br>lat {r.latitude:.5f}, lon {r.longitude:.5f}"
            if audit is not None:
                iqr_pct = 100 * r.iqr_outlier_count / r.observed_speed_count
                popup += (f"<br>zero sentinel: {r.zero_pct:.2f}%"
                          f"<br>ứng viên ngoại lai IQR: {iqr_pct:.2f}% vận tốc hợp lệ"
                          f"<br>Q1–Q3: {r.q1:.1f}–{r.q3:.1f} mph")
            folium.CircleMarker((r.latitude, r.longitude), radius=5, color=color, weight=1,
                                fill=True, fill_color=color, fill_opacity=0.85,
                                tooltip=str(r.sensor_id), popup=folium.Popup(popup, max_width=280)).add_to(group)

    folium.LayerControl(collapsed=False).add_to(fmap)
    Fullscreen().add_to(fmap)
    fmap.save(out)


# --------------------------------------------------------------------------- #
def write_notes(path: Path, hx_checks: list[Check], la_checks: list[Check], edges: pd.DataFrame) -> None:
    def table(checks: list[Check]) -> list[str]:
        rows = ["| Kiểm tra | Kết quả | Chi tiết |", "|---|:---:|---|"]
        rows += [f"| {c.name} | {c.status} | {c.detail or '—'} |" for c in checks]
        return rows

    oneway = edges[~edges["has_reverse"]]
    lines = [
        "# [D2] Kết quả kiểm chứng GIS",
        "",
        "Sinh tự động bởi `04_hcm_dashboard/build_gis_inspection_map.py`. Chạy lại script sau khi [H3] hoặc [T1] cập nhật dữ liệu.",
        "",
        "## Đồ thị Hàng Xanh (`hang_xanh_gis_check.html`)",
        "",
        *table(hx_checks),
        "",
        "Cạnh vẽ nét đứt có mũi tên là cạnh chỉ có một chiều trong `hang_xanh_edges.csv`:",
        "",
        "| edge_id | Chiều | Tên đường | Độ dài (m) |",
        "|---|---|---|---:|",
        *[f"| {r.edge_id} | {r.u_code} → {r.v_code} | {r.name} | {r.length:.0f} |" for r in oneway.itertuples()],
        "",
        "Lưu ý:",
        "",
        "- Cạnh được vẽ thẳng giữa hai nút vì `hang_xanh_edges.csv` không mang geometry. Đường cong thực tế xem trên nền OSM/ảnh vệ tinh.",
        "- Tọa độ nút sau bước gộp nút gần nhau là trọng tâm cụm, nên có thể lệch vài chục mét so với tâm giao lộ trên nền bản đồ.",
        "- Chiều đường cần đối chiếu thực địa trên ảnh vệ tinh hoặc Google Maps trước khi dùng cho Case Study.",
        "",
        "## Cảm biến METR-LA (`metr_la_sensors_map.html`)",
        "",
        *table(la_checks),
        "",
        "Màu điểm theo tỷ lệ zero sentinel của từng cảm biến lấy từ `metr-la_sensor_audit.csv` ([T3]).",
        "Mọi cảm biến đều có tối thiểu khoảng 6,3% giá trị 0. Phần nền này đến từ các mốc thời gian mà toàn bộ 207 cảm biến cùng báo 0 (mất tín hiệu toàn mạng), nên chỉ những điểm cam/đỏ mới là cảm biến lỗi riêng lẻ.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    project = Path(__file__).resolve().parents[1]
    ingestion = project / "01_data_ingestion"
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ingestion-dir", type=Path, default=ingestion)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--strict", action="store_true", help="Trả mã thoát 1 nếu có kiểm tra FAIL")
    args = parser.parse_args()

    nodes = read_csv(args.ingestion_dir / "hang_xanh_nodes.csv")
    edges = read_csv(args.ingestion_dir / "hang_xanh_edges.csv")
    sensors = read_csv(args.ingestion_dir / "graph_sensor_locations.csv")
    audit_path = args.ingestion_dir / "metr-la_sensor_audit.csv"
    audit = read_csv(audit_path) if audit_path.exists() else None

    hx_checks, edges = check_hang_xanh(nodes, edges)
    la_checks = check_metr_la(sensors, audit)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    build_hang_xanh_map(nodes, edges, args.output_dir / "hang_xanh_gis_check.html")
    build_metr_la_map(sensors, audit, args.output_dir / "metr_la_sensors_map.html")
    write_notes(args.output_dir / "gis_inspection_notes.md", hx_checks, la_checks, edges)

    for c in hx_checks + la_checks:
        print(f"[{c.status}] {c.name}{' — ' + c.detail if c.detail else ''}")
    failed = [c for c in hx_checks + la_checks if c.status == "FAIL"]
    print(f"Đã ghi bản đồ và ghi chú vào {args.output_dir} ({len(failed)} kiểm tra FAIL)")
    return 1 if (failed and args.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
