# Nguồn dữ liệu và quy tắc ghép dữ liệu

## METR-LA và PEMS-BAY

- Bản dữ liệu dùng trong repo: các CSV công khai trong [Zenodo record 5146275](https://zenodo.org/records/5146275), phát hành với giấy phép CC BY 4.0. Script tải: `download_traffic_datasets.py`.
- METR-LA có 34.272 mốc thời gian × 207 cảm biến; PEMS-BAY có 52.116 × 325. CSV gốc là ma trận rộng, mỗi hàng là một thời điểm 5 phút và mỗi cột là một cảm biến.
- Tọa độ 207 cảm biến METR-LA và adjacency METR-LA lấy từ repo [DCRNN của Li et al.](https://github.com/liyaguang/DCRNN/tree/602afd9d767d3aa1c9b3eac51710d6aeee12c227), ghim ở commit `602afd9d767d3aa1c9b3eac51710d6aeee12c227`. PEMS-BAY metadata và adjacency lấy cùng Zenodo record.
- CSV được giữ nguyên tại `data/raw/`. Bản Parquet chuẩn hóa nằm tại `data/interim/`, dạng dài với `timestamp` độ phân giải microsecond (`timestamp[us]`), `sensor_id`, `speed` và nén Snappy để tương thích Spark. Không commit file dữ liệu vào Git.
- Tài liệu gốc DCRNN dùng định dạng HDF5 cho chuỗi vận tốc. Bản đã tải ở đây là bản CSV tương thích về ma trận thời gian × cảm biến; vì pipeline nhóm dùng Parquet nên không tạo thêm bước HDF5 trung gian.

## Thời tiết Hàng Xanh

- Script gọi [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api) với tọa độ tâm Hàng Xanh `(10.8012, 106.7114)`, múi giờ `Asia/Ho_Chi_Minh`, các biến mưa, nhiệt độ, độ ẩm và tốc độ gió.
- API trả tọa độ lưới mô hình gần nhất riêng với tọa độ yêu cầu. CSV lưu cả `requested_latitude/longitude` và `latitude/longitude` trả về, cùng timestamp ISO-8601 có UTC+7.
- Mẫu hiện tại bao phủ 90 ngày, 2.160 giờ; lần thu thập ngày 18/11/2026 ghi nhận 88 ngày có mưa, 2 ngày khô, mưa tối đa 10,4 mm/giờ và 41,6 mm/ngày.
- Đây là dữ liệu khí tượng Hàng Xanh, không phải dữ liệu tốc độ giao thông tại Hàng Xanh. Không ghép nó trực tiếp với METR-LA hoặc PEMS-BAY để hồi quy: các bộ giao thông ở California và có giai đoạn quan sát khác. Phân tích OLS cần chuỗi giao thông và thời tiết cùng khu vực, cùng khoảng thời gian; nếu chưa có tốc độ giao thông Hàng Xanh, chỉ trình bày thời tiết như bối cảnh GIS và không kết luận quan hệ mưa–tốc độ tại TP.HCM.
