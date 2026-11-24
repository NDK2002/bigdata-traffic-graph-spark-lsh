# CHUYÊN ĐỀ LÝ THUYẾT: BIỂU DIỄN ĐỒ THỊ KHÔNG–THỜI GIAN $G = (V, E, W)$ & MA TRẬN TRỌNG SỐ GAUSSIAN
## TÀI LIỆU CƠ SỞ TOÁN HỌC & TRÍCH XUẤT THỰC ĐỊA PHỤC VỤ CHƯƠNG 2 BÁO CÁO ĐỒ ÁN
**Môn học:** Phân tích Dữ liệu Lớn (*Big Data Analytics*) — Học kỳ 1, Năm học 2026–2027  
**Nhóm thực hiện:** Nhóm 11 — CITD / Trường Đại học Công nghệ Thông tin, ĐHQG-HCM  
**Thành viên phụ trách:** Trần Hoàng Hôn (Trưởng nhóm / PM & Đồ thị TP.HCM — MSSV: `26410046`)  
**Tài liệu tham chiếu:** [literature_survey_notes.md](./literature_survey_notes.md) | [extract_hang_xanh_graph.py](../01_data_ingestion/extract_hang_xanh_graph.py) | [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md](../De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md)

---

## 1. BỐI CẢNH VÀ ĐỘNG LỰC TOÁN HỌC

Trong các hệ thống phân tích dữ liệu giao thông thông minh (*Intelligent Transportation Systems – ITS*), dữ liệu tốc độ và lưu lượng không tồn tại độc lập ở các điểm rời rạc trên không gian Euclid, mà bị ràng buộc ngặt nghèo bởi cấu trúc liên kết vật lý của mạng lưới đường bộ (*road road network topology*). 

Nếu xem nhẹ cấu trúc topo mạng lưới và xử lý dữ liệu dưới dạng chuỗi thời gian đơn biến (*univariate time series*) thông thường (như mô hình ARIMA, LSTM tiêu chuẩn), hệ thống sẽ hoàn toàn bỏ qua các quy luật vật lý then chốt:
1. **Tính định hướng và bất đối xứng (*Directed Asymmetry*):** Các tuyến đường một chiều, làn đường chuyên dụng, và dải phân cách cứng khiến dòng xe chỉ có thể di chuyển theo một chiều xác định. Ùn tắc tại điểm hạ lưu (*downstream*) sẽ dội ngược trở lại thượng lưu (*upstream*), nhưng luồng giao thông ở chiều ngược lại phía bên kia dải phân cách hoàn toàn không bị ảnh hưởng.
2. **Khoảng cách topo so với khoảng cách đường chim bay (*Network Distance vs. Euclidean Distance*):** Hai vị trí địa lý có thể cách nhau chỉ vài chục mét theo đường chim bay (khoảng cách Euclidean hoặc Haversine), ví dụ như hai bờ đối diện của Kênh Nhiêu Lộc – Thị Nghè hay hai bên chân cầu vượt ngã tư, nhưng phương tiện giao thông muốn di chuyển qua lại bắt buộc phải đi vòng qua cầu hoặc nút giao hợp pháp với quãng đường dài gấp hàng chục lần. Việc áp dụng khoảng cách Euclidean thông thường sẽ dẫn đến kết luận sai lệch nghiêm trọng về mối tương quan giao thông.
3. **Phân rã tương quan theo không gian (*Spatial Correlation Decay*):** Tuân thủ Định luật thứ nhất của Địa lý học Tobler (*Tobler's First Law of Geography*): *"Mọi sự vật đều có mối liên hệ với nhau, nhưng những sự vật ở gần nhau thì có liên hệ chặt chẽ hơn những sự vật ở xa"*. Ảnh hưởng của ùn tắc giảm dần theo khoảng cách di chuyển trên mạng lưới đường bộ.

Do đó, việc mô hình hóa mạng lưới giao thông dưới dạng một **Đồ thị không gian có hướng và có trọng số (*Directed Weighted Spatial Graph*) $G = (V, E, W)$** kết hợp với **Hàm nhân Gaussian (*Gaussian Kernel*)** là nền tảng toán học tối ưu, cung cấp bộ khung cấu trúc để tích hợp vào pipeline xử lý dữ liệu lớn trên Apache Spark và thuật toán tìm kiếm láng giềng xấp xỉ LSH/ANN.

---

## 2. ĐỊNH NGHĨA HÌNH HỌC & TOÁN HỌC CỦA ĐỒ THỊ $G = (V, E, W)$

### 2.1. Không gian Đồ thị Cơ sở
Một mạng lưới giao thông đô thị được biểu diễn hình thức bởi bộ ba:
$$G = (V, E, W)$$

Trong đó:
* **Tập đỉnh $V = \{v_1, v_2, \dots, v_N\}$:** Đại diện cho $N$ thực thể đo lường hoặc các điểm nút giao thông.
  * Trong tập dữ liệu chuẩn quốc tế (METR-LA / PEMS-BAY): Mỗi đỉnh $v_i \in V$ tương ứng với một cảm biến vòng từ lặp (*loop detector*) được cấy cố định trên mặt đường cao tốc ($N = 207$ đối với METR-LA; $N = 325$ đối với PEMS-BAY).
  * Trong bài toán mô phỏng TP.HCM (Khu vực Ngã tư Hàng Xanh): Mỗi đỉnh $v_i \in V$ đại diện cho một nút giao thông trọng điểm hoặc giao lộ giữa các trục đường huyết mạch ($N = 20$ nút sau khi rút gọn từ đồ thị OpenStreetMap).
* **Tập cung có hướng $E \subseteq V \times V$:** Tập hợp các đoạn đường hoặc kết nối vật lý liên thông cho phép phương tiện di chuyển từ đỉnh $v_i$ sang đỉnh $v_j$. Cung có hướng $e_{ij} = (v_i, v_j) \in E$ tồn tại khi và chỉ khi có dòng chảy giao thông hợp pháp trực tiếp từ $v_i$ đến $v_j$. Đối với các tuyến đường hai chiều, liên kết được biểu diễn bằng hai cung có hướng đối ngẫu: $(v_i, v_j) \in E$ và $(v_j, v_i) \in E$.
* **Ma trận trọng số không gian $W \in \mathbb{R}^{N \times N}$:** Ma trận thực vuông phản ánh cường độ tương tác hoặc mức độ phụ thuộc không gian giữa các cặp đỉnh. Phần tử $W_{ij} \ge 0$ định lượng mức độ ảnh hưởng của giao thông tại đỉnh $v_i$ lên đỉnh $v_j$.

### 2.2. Đo lường Khoảng cách Mạng lưới theo Thuật toán Dijkstra
Để phản ánh chính xác cấu trúc hạ tầng đường bộ, khoảng cách giữa hai đỉnh bất kỳ $v_i, v_j \in V$ không được tính bằng khoảng cách tọa độ phẳng, mà được định nghĩa là độ dài của đường đi ngắn nhất (*Shortest Path Length*) trên đồ thị có hướng:

$$\text{dist}(v_i, v_j) = \min_{p \in \mathcal{P}(v_i, v_j)} \sum_{e \in p} \text{length}(e)$$

Trong đó $\mathcal{P}(v_i, v_j)$ là tập hợp tất cả các đường đi có hướng từ $v_i$ đến $v_j$, và $\text{length}(e)$ là độ dài vật lý (đo bằng mét) của đoạn đường $e$. Khoảng cách này được tính toán hiệu quả thông qua giải thuật **Dijkstra nguồn đơn (*Single-Source Shortest Path – SSSP*)**:

$$\text{dist}(v_i, v_j) = \begin{cases} \text{Dijkstra\_Distance}(v_i, v_j), & \text{nếu tồn tại đường đi từ } v_i \to v_j \\ +\infty, & \text{nếu không có đường đi liên thông} \end{cases}$$

**Tính chất bất đối xứng:** Do sự hiện diện của các tuyến đường một chiều, nhìn chung:
$$\text{dist}(v_i, v_j) \neq \text{dist}(v_j, v_i)$$
Điều này phản ánh chính xác thực tế vận hành đô thị: thời gian và quãng đường xe chạy từ Hàng Xanh về Bến xe Miền Đông qua trục Đinh Bộ Lĩnh hoàn toàn khác với chiều ngược lại đi qua trục Xô Viết Nghệ Tĩnh.

---

## 3. THIẾT KẾ MA TRẬN TRỌNG SỐ GAUSSIAN VỚI NGƯỠNG CẮT ($\kappa, \epsilon$)

### 3.1. Công thức Hàm nhân Gaussian
Theo chuẩn mực học thuật của các công trình Spatio-Temporal Graph Neural Networks hàng đầu thế giới (tiêu biểu là mô hình DCRNN của Li et al., *ICLR 2018* [1] và Graph WaveNet của Wu et al., *IJCAI 2019*), mối quan hệ không gian giữa hai nút giao thông được tham số hóa thông qua hàm nhân Gaussian chuẩn hóa (*Thresholded Gaussian Kernel*):

$$W_{ij} = \begin{cases} \exp\left(-\dfrac{\text{dist}(v_i, v_j)^2}{\sigma^2}\right), & \text{nếu } \text{dist}(v_i, v_j) \le \kappa \quad \text{và} \quad \exp\left(-\dfrac{\text{dist}(v_i, v_j)^2}{\sigma^2}\right) \ge \epsilon \\ 1.0, & \text{nếu } i = j \\ 0.0, & \text{ngược lại} \end{cases}$$

### 3.2. Ý nghĩa & Vai trò của các Siêu tham số Toán học

1. **Độ lệch chuẩn khoảng cách ($\sigma$):**
   * $\sigma$ đóng vai trò là hệ số tỷ lệ co dãn băng thông (*bandwidth parameter*). Giá trị $\sigma$ quyết định tốc độ suy giảm của trọng số tương quan theo khoảng cách.
   * Để hàm nhân phản ánh khách quan cấu trúc dữ liệu mà không bị áp đặt cảm tính, $\sigma$ được tính toán tự động bằng **độ lệch chuẩn mẫu của toàn bộ các khoảng cách mạng lưới hữu hạn và khác 0** trên toàn đồ thị:
     $$\sigma = \sqrt{\frac{1}{|P|} \sum_{(v_i, v_j) \in P} \left(\text{dist}(v_i, v_j) - \mu_{\text{dist}}\right)^2}$$
     với $P = \{(v_i, v_j) \in V \times V \mid i \neq j \text{ và } \text{dist}(v_i, v_j) < \infty\}$, và $\mu_{\text{dist}}$ là khoảng cách mạng lưới trung bình.
   * Trong thực nghiệm mạng lưới Hàng Xanh, qua phân tích 135 cặp đường đi liên thông, độ lệch chuẩn thực tế đạt $\sigma \approx 500.0\text{ m}$ (trùng khớp với bán kính phục vụ trung bình của một nút giao đô thị).

2. **Ngưỡng bán kính vật lý cục bộ ($\kappa$):**
   * $\kappa$ đại diện cho bán kính cắt vật lý (*spatial cutoff radius*). Các nút giao có khoảng cách vượt quá $\kappa$ sẽ bị triệt tiêu tương quan trực tiếp, do hiệu ứng giao thông giữa hai nút cách nhau quá xa thường mang tính gián tiếp thông qua các nút trung gian thay vì tương tác trực tiếp. Trong mô hình đồ án, $\kappa$ được giới hạn trong phạm vi khảo sát bán kính $1.000\text{ m}$.

3. **Ngưỡng thưa hóa ma trận ($\epsilon$):**
   * Hàm mũ $\exp(-x)$ mang giá trị tiệm cận 0 nhưng không bao giờ đạt giá trị 0 tuyệt đối về mặt toán học. Nếu không có ngưỡng cắt $\epsilon$, ma trận trọng số $W$ sẽ trở thành một **ma trận đặc (*dense matrix*)** kích thước $N \times N$, gây lãng phí bộ nhớ và tạo ra hàng nghìn liên kết giả tạo với trọng số cực nhỏ ($10^{-5} - 10^{-8}$).
   * Thiết lập $\epsilon = 0.1$ giúp chuyển đổi $W$ thành **ma trận thưa (*sparse matrix*)**. Các cặp nút có $W_{ij} < 0.1$ được gán cứng về $0.0$, giúp tối ưu hóa bộ nhớ RAM, cho phép lưu trữ dưới dạng định dạng thưa (CSR/COO) và đẩy nhanh tốc độ tính toán phân tán trên Spark.

---

## 4. HIỆN THỰC THỰC NGHIỆM: TRÍCH XUẤT ĐỒ THỊ NGÃ TƯ HÀNG XANH (TP.HCM) TỪ OSMNX

Nhằm hiện thực hóa cơ sở lý thuyết vào bối cảnh thực tế tại Việt Nam, đề tài đã thiết kế pipeline trích xuất đồ thị tự động từ dữ liệu bản đồ mở OpenStreetMap thông qua thư viện `OSMnx` (Boeing, 2017 [5]) tại khu vực Ngã tư Hàng Xanh (quận Bình Thạnh, TP.HCM) — điểm nóng giao thông huyết mạch của thành phố.

```
          [Bắc: Bến xe Miền Đông cũ]
                   | (Đinh Bộ Lĩnh / XVNT)
                   v
[Tây: Bạch Đằng] ---> [NGÃ TƯ HÀNG XANH] ---> [Đông: Cầu Sài Gòn / D2]
                   ^        |
                   |        v
           [Nam: Cầu Điện Biên Phủ / Trung tâm Q1]
```

### 4.1. Thông số Kỹ thuật Vùng Khảo sát
* **Tọa độ tâm khảo sát:** Vĩ độ $10.8012^\circ\text{N}$, Kinh độ $106.7114^\circ\text{E}$ (Ngã tư Hàng Xanh).
* **Bán kính quét (*Radius*):** $R = 1.000\text{ m}$ bao quát toàn bộ 6 trục đường huyết mạch:
  1. *Xô Viết Nghệ Tĩnh:* Trục xuyên tâm nối dài từ Thị Nghè ra Bến xe Miền Đông và Thanh Đa.
  2. *Đinh Bộ Lĩnh:* Trục đường một chiều gánh tải chính từ hướng Đông Bắc đổ vào trung tâm Quận 1.
  3. *Bạch Đằng:* Tuyến kết nối huyết mạch từ Bình Thạnh sang Phú Nhuận và Sân bay Tân Sơn Nhất.
  4. *Điện Biên Phủ:* Trục đại lộ đô thị rộng 8–10 làn xe, kết nối trực tiếp với Cầu Sài Gòn và Xa lộ Hà Nội.
  5. *Nguyễn Gia Trí (Đường D2 cũ):* Khu phức hợp các trường đại học (HUTECH, Giao thông Vận tải, Ngoại thương) có mật độ sinh viên và phương tiện cá nhân rất cao.
  6. *Ung Văn Khiêm:* Trục ven sông kết nối cụm đô thị Thanh Đa và Tân Cảng.
  *Kèm hệ thống các công trình hạ tầng đặc thù:* Cầu vượt Hàng Xanh, Cầu Đinh Bộ Lĩnh, Cầu Sơn, Cầu Văn Thánh.

### 4.2. Rút gọn & Chuẩn hóa Đồ thị
Đồ thị giao thông trích xuất thô từ OpenStreetMap chứa hàng nghìn nút giao nhỏ (bao gồm các đoạn cong lượn của vỉa hè, ngõ hẻm và dải phân cách). Nhóm đã tiến hành thuật toán lọc và rút gọntopo:
1. **Lọc lớp đường (*Highway Filtering*):** Chỉ giữ lại các cấp đường giao thông chính (`trunk`, `primary`, `secondary`, `tertiary` và các nhánh nối `link`).
2. **Gộp cụm nút giao phức hợp (*Complex Intersection Consolidation*):** Gom các điểm giao cắt nhỏ lẻ thuộc cùng một ngã tư vật lý thành một nút đại diện duy nhất.
3. **Chiếu hệ tọa độ phẳng mét (*UTM Projection*):** Chuyển đổi tọa độ từ WGS-84 (EPSG:4326) sang hệ tọa độ phẳng UTM Zone 48N (EPSG:32648) để đảm bảo phép đo khoảng cách độ dài cạnh đường đạt độ chính xác đến từng centimet.

Kết quả rút gọn thu được **đồ thị $G_{\text{HX}}$ chuẩn hóa gồm $N = 20$ nút giao trọng điểm (`HX_01` đến `HX_20`) và 37 cung đường có hướng (`E_01` đến `E_37`)**.

### 4.3. Danh mục 20 Nút Giao Trọng điểm Mạng lưới Hàng Xanh

*Bảng 2.1: Danh mục 20 nút giao trọng điểm cụm Hàng Xanh được trích xuất và chuẩn hóa.*

| Mã nút (`node_code`) | Tọa độ (Vĩ độ, Kinh độ) | Các trục đường giao cắt chính | Cấp đường OSM | Khoảng cách tới tâm (m) |
| :---: | :---: | :--- | :--- | :---: |
| **`HX_01`** | $10.801405, 106.711122$ | Điện Biên Phủ – Xô Viết Nghệ Tĩnh (Tâm Ngã tư Hàng Xanh) | `primary, trunk` | $44.7$ |
| **`HX_02`** | $10.802115, 106.712718$ | Điện Biên Phủ – Đường D1 (Nguyễn Văn Thương) | `primary, tertiary` | $208.8$ |
| **`HX_03`** | $10.803120, 106.710350$ | Xô Viết Nghệ Tĩnh – Bạch Đằng | `primary, secondary` | $234.7$ |
| **`HX_04`** | $10.799810, 106.709210$ | Điện Biên Phủ – Cầu vượt Hàng Xanh (Nhánh Tây) | `primary, trunk` | $296.7$ |
| **`HX_05`** | $10.803512, 106.713501$ | Điện Biên Phủ – Nguyễn Gia Trí (D2) | `primary, secondary` | $367.1$ |
| **`HX_06`** | $10.804210, 106.708915$ | Bạch Đằng – Đinh Bộ Lĩnh | `primary, secondary` | $412.3$ |
| **`HX_07`** | $10.805118, 106.711820$ | Xô Viết Nghệ Tĩnh – D5 | `primary, tertiary` | $489.6$ |
| **`HX_08`** | $10.798120, 106.707510$ | Điện Biên Phủ – Cầu Văn Thánh | `primary, trunk` | $542.8$ |
| **`HX_09`** | $10.805910, 106.714210$ | Nguyễn Gia Trí (D2) – Ung Văn Khiêm | `secondary, tertiary` | $612.4$ |
| **`HX_10`** | $10.806815, 106.709812$ | Đinh Bộ Lĩnh – Bùi Đình Túy | `primary, secondary` | $685.2$ |
| **`HX_11`** | $10.807510, 106.712910$ | Xô Viết Nghệ Tĩnh – Ung Văn Khiêm | `primary, secondary` | $741.5$ |
| **`HX_12`** | $10.808210, 106.708510$ | Đinh Bộ Lĩnh – Cổng Bến xe Miền Đông cũ | `primary, trunk` | $820.1$ |
| **`HX_13`** | $10.796510, 106.705810$ | Điện Biên Phủ – Ngã ba Hàng Xanh Nam | `primary` | $865.4$ |
| **`HX_14`** | $10.809120, 106.711210$ | Đinh Bộ Lĩnh – Cầu Đinh Bộ Lĩnh | `primary` | $912.7$ |
| **`HX_15`** | $10.804910, 106.706120$ | Bạch Đằng – Phan Chu Trinh | `secondary` | $935.0$ |
| **`HX_16`** | $10.809815, 106.713810$ | Xô Viết Nghệ Tĩnh – Cầu Sơn | `primary` | $962.3$ |
| **`HX_17`** | $10.806210, 106.717110$ | Ung Văn Khiêm – Nguyễn Gia Trí nối dài | `secondary` | $978.9$ |
| **`HX_18`** | $10.801120, 106.719810$ | Điện Biên Phủ – Chân Cầu Sài Gòn | `trunk, primary` | $985.4$ |
| **`HX_19`** | $10.795210, 106.704210$ | Điện Biên Phủ – Nguyễn Cửu Vân | `primary` | $991.2$ |
| **`HX_20`** | $10.807120, 106.704510$ | Bạch Đằng – Lê Quang Định (Chợ Bà Chiểu) | `secondary` | $998.6$ |

---

## 5. ĐẶC TRƯNG CỦA MA TRẬN TRỌNG SỐ GAUSSIAN THỰC TẾ TẠI HÀNG XANH

Ma trận kề nhị phân $A \in \{0, 1\}^{20 \times 20}$ và ma trận trọng số Gaussian $W \in [0, 1]^{20 \times 20}$ được tính toán tự động qua script [extract_hang_xanh_graph.py](../01_data_ingestion/extract_hang_xanh_graph.py). Phân tích cấu trúc dữ liệu xuất bản tại [hang_xanh_adjacency_gaussian.csv](../01_data_ingestion/hang_xanh_adjacency_gaussian.csv) cho thấy các đặc tính quan trọng sau:

```
Trích xuất Ma trận Trọng số Gaussian W (5 nút trung tâm đầu tiên):
        HX_01   HX_02   HX_03   HX_04   HX_05
HX_01  1.0000  0.8987  0.8651  0.7212  0.6215
HX_02  0.8742  1.0000  0.5719  0.4145  0.3276
HX_03  0.8611  0.6010  1.0000  0.3991  0.3139
HX_04  0.7209  0.4458  0.9018  1.0000  0.2036
HX_05  0.6205  0.3551  0.3172  0.2032  1.0000
```

1. **Đường chéo chính $W_{ii} = 1.0$:** Đại diện cho mối tự tương quan (*self-loop connection*) hoàn hảo của từng nút giao với chính nó.
2. **Tính bất đối xứng thực tế ($W_{ij} \neq W_{ji}$):**
   * Ví dụ: $W_{\text{HX\_01}, \text{HX\_02}} = 0.8987$, trong khi $W_{\text{HX\_02}, \text{HX\_01}} = 0.8742$. Sự chênh lệch này xuất phát từ việc chiều dài cung đường thực tế theo phân luồng một chiều quanh vòng xoay và chân cầu vượt có độ chênh lệch nhất định.
3. **Mức độ thưa hóa tối ưu (*Optimal Sparsity*):**
   * Tổng số phần tử của ma trận: $20 \times 20 = 400$ ô.
   * Số ô có trọng số hữu hạn vượt ngưỡng $\epsilon = 0.1$: **135 ô (đạt tỷ lệ thưa $33.8\%$)**.
   * Số ô bằng $0.0$: **265 ô (chiếm $66.2\%$)**.
   * Tỷ lệ thưa $33.8\%$ là con số lý tưởng trong biểu diễn đồ thị không–thời gian, vừa loại bỏ triệt để các kết nối nhiễu ở khoảng cách xa, vừa bảo toàn đầy đủ các hành lang lan truyền lưu lượng chính giữa các nút lân cận.

---

## 6. VAI TRÒ CỦA $G = (V, E, W)$ TRONG TOÀN BỘ PIPELINE ĐỒ ÁN

Ma trận trọng số Gaussian $W$ không chỉ là một công cụ biểu diễn tĩnh, mà giữ vai trò cốt lõi trong các phân hệ tiếp theo của đồ án:

```
                    MẠNG LƯỚI ĐỒ THỊ G = (V, E, W)
                                 |
        +------------------------+------------------------+
        |                                                 |
        v                                                 v
[RÀNG BUỘC KHÔNG GIAN CHO LSH/ANN]           [PHÂN TÍCH TƯƠNG QUAN THỜI TIẾT TP.HCM]
  - Giảm không gian tìm kiếm vector           - Kết hợp dữ liệu mưa Open-Meteo
  - Chỉ so khớp tương đồng giữa               - Đánh giá suy giảm vận tốc theo
    các nút có liên thông W_ij > 0              từng hành lang giao thông thực tế
  - Tránh match nhầm các mẫu tốc              - Tuyệt đối không hồi quy chéo với
    độ ở hai khu vực hoàn toàn độc lập          dữ liệu California (METR-LA)
```

1. **Ràng buộc không gian cho thuật toán LSH/ANN (*Spatial Pruning*):**
   * Khi tìm kiếm trạng thái giao thông tương đồng trên quy mô triệu vector, việc so khớp mù (*blind matching*) giữa mọi cặp cảm biến sẽ gây tốn kém tính toán và tạo ra các kết quả sai lệch vật lý (ví dụ: gán ghép mẫu ùn tắc của một đường cao tốc hướng Bắc với một tuyến đường nhánh hướng Nam chỉ vì chúng có cùng tốc độ 15 mph).
   * Ma trận $W$ cung cấp bộ lọc không gian: Hệ thống chỉ tiến hành băm LSH và tìm kiếm láng giềng gần đúng giữa các vector đặc trưng thuộc các nút có liên thông không gian ($W_{ij} \ge \epsilon$).
2. **Chuẩn bị đầu vào cho Biến đổi Đồ thị Không gian (*Graph Convolution / Spatial Diffusion*):**
   * Ma trận $W$ là nền tảng để tính toán **Ma trận Laplacian Đồ thị Chuẩn hóa (*Normalized Graph Laplacian*)**:
     $$L = I_N - D^{-1/2} W D^{-1/2}$$
     với $D$ là ma trận đường chéo bậc đỉnh (*degree matrix*): $D_{ii} = \sum_j W_{ij}$. Phép biến đổi này mở đường cho các nghiên cứu mở rộng áp dụng Graph Neural Networks trong tương lai.
3. **Cách ly khoa học với dữ liệu Benchmark:**
   * Nghiên cứu tuân thủ nghiêm ngặt nguyên tắc phương pháp luận: Đồ thị $G_{\text{HX}}$ và dữ liệu thời tiết Open-Meteo TP.HCM chỉ được dùng cho bài toán **Case Study mô phỏng phân tích thực tế tại TP.HCM**, hoàn toàn độc lập và không bao giờ hồi quy chéo với dữ liệu tốc độ giao thông thu thập tại Los Angeles (`METR-LA`) hay Vùng Vịnh California (`PEMS-BAY`).
