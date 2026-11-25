# [D2] Kết quả kiểm chứng GIS

Sinh tự động bởi `04_hcm_dashboard/build_gis_inspection_map.py`. Chạy lại script sau khi [H3] hoặc [T1] cập nhật dữ liệu.

## Đồ thị Hàng Xanh (`hang_xanh_gis_check.html`)

| Kiểm tra | Kết quả | Chi tiết |
|---|:---:|---|
| Số nút trong ngưỡng 15–25 | PASS | 20 nút |
| node_id / node_code duy nhất | PASS | — |
| dist_center_m khớp khoảng cách haversine tới tâm (sai lệch ≤ 25 m) | PASS | sai lệch lớn nhất 4.9 m |
| Nút nằm trong bán kính trích xuất 1000 m | WARN | ngoài bán kính: HX_20 (1122 m) |
| utm_x/utm_y có giá trị (UTM 48N) | FAIL | 20/20 nút có utm_x = utm_y = 0 |
| Đầu mút cạnh tồn tại trong bảng nút | PASS | 37 cạnh |
| Không có self-loop | PASS | 0 self-loop |
| Không trùng cạnh (u, v) | PASS | 0 cạnh trùng |
| in_degree/out_degree khớp bảng cạnh | PASS | — |
| Không có nút cô lập | PASS | — |
| Nút chỉ đi ra / chỉ đi vào (biên đồ thị) | PASS | chỉ ra: HX_14 · chỉ vào: HX_20 |
| Cờ bidirectional_pair khớp sự tồn tại cạnh ngược | PASS | — |
| Cạnh oneway=False có cạnh ngược | PASS | — |
| Hai chiều của cùng cặp nút thống nhất cờ oneway | FAIL | HX_10↔HX_15 |
| Độ dài cạnh ≥ 90% khoảng cách đường chim bay | PASS | tỷ lệ nhỏ nhất 0.932 |

Cạnh vẽ nét đứt có mũi tên là cạnh chỉ có một chiều trong `hang_xanh_edges.csv`:

| edge_id | Chiều | Tên đường | Độ dài (m) |
|---|---|---|---:|
| E_05 | HX_01 → HX_11 | Cầu Sơn / Xô Viết Nghệ Tĩnh | 578 |
| E_08 | HX_02 → HX_08 | Điện Biên Phủ | 312 |
| E_13 | HX_04 → HX_03 | Đinh Bộ Lĩnh | 202 |
| E_19 | HX_07 → HX_02 | Điện Biên Phủ | 215 |
| E_21 | HX_08 → HX_07 | Điện Biên Phủ | 82 |
| E_27 | HX_11 → HX_18 | Xô Viết Nghệ Tĩnh | 314 |
| E_31 | HX_14 → HX_04 | Đinh Bộ Lĩnh / Cầu Đinh Bộ Lĩnh | 595 |
| E_35 | HX_17 → HX_20 | Xô Viết Nghệ Tĩnh | 201 |
| E_36 | HX_18 → HX_15 | Ung Văn Khiêm | 491 |

Lưu ý:

- Cạnh được vẽ thẳng giữa hai nút vì `hang_xanh_edges.csv` không mang geometry. Đường cong thực tế xem trên nền OSM/ảnh vệ tinh.
- Tọa độ nút sau bước gộp nút gần nhau là trọng tâm cụm, nên có thể lệch vài chục mét so với tâm giao lộ trên nền bản đồ.
- Chiều đường cần đối chiếu thực địa trên ảnh vệ tinh hoặc Google Maps trước khi dùng cho Case Study.

## Cảm biến METR-LA (`metr_la_sensors_map.html`)

| Kiểm tra | Kết quả | Chi tiết |
|---|:---:|---|
| Đủ 207 cảm biến | PASS | 207 dòng |
| sensor_id duy nhất | PASS | — |
| Không trùng tọa độ | PASS | 0 điểm trùng |
| Tọa độ nằm trong vùng Los Angeles (33.6, 34.5, -118.8, -117.7) | PASS | lat 34.0430–34.2216, lon -118.5368–-118.1829 |
| sensor_id khớp metr-la_sensor_audit.csv | PASS | 207/207 |

Màu điểm theo tỷ lệ zero sentinel của từng cảm biến lấy từ `metr-la_sensor_audit.csv` ([T3]).
Mọi cảm biến đều có tối thiểu khoảng 6,3% giá trị 0. Phần nền này đến từ các mốc thời gian mà toàn bộ 207 cảm biến cùng báo 0 (mất tín hiệu toàn mạng), nên chỉ những điểm cam/đỏ mới là cảm biến lỗi riêng lẻ.
