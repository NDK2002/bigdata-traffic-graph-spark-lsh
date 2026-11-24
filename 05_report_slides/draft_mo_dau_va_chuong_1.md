# BẢN THẢO CHI TIẾT: PHẦN MỞ ĐẦU & CHƯƠNG 1
## BÁO CÁO ĐỒ ÁN MÔN HỌC: PHÂN TÍCH DỮ LIỆU LỚN (`BIG DATA ANALYTICS`)
### ĐỀ TÀI: XÂY DỰNG PIPELINE XỬ LÝ DỮ LIỆU LỚN MẠNG LƯỚI GIAO THÔNG TRÊN CẤU TRÚC ĐỒ THỊ KHÔNG–THỜI GIAN BẰNG APACHE SPARK VÀ KỸ THUẬT TÌM KIẾM LÁNG GIỀNG GẦN ĐÚNG (LSH/ANN): THỰC NGHIỆM ĐỐI CHỨNG TRÊN METR-LA/PEMS-BAY VÀ ỨNG DỤNG PHÂN TÍCH TẠI TP.HCM KẾT HỢP THỜI TIẾT

> **Tác giả chấp bút:** Trần Hoàng Hôn (`26410046`) — Trưởng nhóm / PM  
> **Đơn vị đào tạo:** Khoa Mạng máy tính & Truyền thông — Trường Đại học Công nghệ Thông tin (UIT) – ĐHQG-HCM (CITD)  
> **Giảng viên hướng dẫn:** TS. Hà Minh Tân  
> **Tài liệu tham chiếu:** [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md](../De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.md) | [literature_survey_notes.md](./literature_survey_notes.md)  
> **Mục đích:** Cung cấp toàn văn nội dung chuẩn học thuật phục vụ điền vào [Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx](../Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx) (đáp ứng mục tiêu $\ge 40$ trang toàn văn).

---

# PHẦN MỞ ĐẦU

## 1. Bối cảnh và Tính cấp thiết của Đề tài
Sự bùng nổ của quá trình đô thị hóa tại các siêu đô thị trên thế giới và tại Việt Nam (đặc biệt là Thành phố Hồ Chí Minh) đã dẫn đến áp lực khổng lồ lên hạ tầng giao thông đường bộ. Tình trạng ùn tắc kéo dài, tai nạn giao thông và sự lãng phí nhiên liệu, phát thải khí nhà kính đang gây ra những thiệt hại kinh tế hàng tỷ USD mỗi năm. Nhằm nâng cao hiệu quả vận hành và điều phối giao thông, các Hệ thống Giao thông Thông minh (Intelligent Transportation Systems — ITS) hiện đại đã đẩy mạnh việc triển khai mạng lưới cảm biến đo đếm tự động (Loop Detectors, Camera AI, thiết bị định vị GPS hành trình) dày đặc trên các trục đường huyết mạch.

Các cảm biến này liên tục thu nhận các thông số vận tốc, lưu lượng và mật độ dòng xe theo các chu kỳ thời gian rất ngắn (thường là $5$ phút một lần). Kết quả là một khối lượng dữ liệu khổng lồ, cập nhật liên tục và biến thiên phức tạp được sinh ra mỗi ngày. Đây là một minh chứng điển hình của **Dữ liệu lớn (Big Data)** trong lĩnh vực giao thông thông minh. 

Tuy nhiên, việc khai thác và xử lý khối dữ liệu này đặt ra những thách thức kỹ thuật to lớn:
1. **Nút thắt cổ chai về tài nguyên bộ nhớ:** Các công cụ phân tích dữ liệu đơn máy phổ biến như Python Pandas, Scikit-learn hay NumPy hoàn toàn phụ thuộc vào bộ nhớ RAM vật lý của một máy trạm. Khi khối lượng dữ liệu tăng từ vài trăm nghìn lên hàng triệu và hàng chục triệu bản ghi (chẳng hạn bộ dữ liệu chuẩn quốc tế `METR-LA` với hơn $7.09\text{ triệu}$ dòng hay `PEMS-BAY` với gần $16.94\text{ triệu}$ dòng), các thao tác xoay trục (unpivot/melt), tính toán cửa sổ trượt (rolling window) và làm sạch dữ liệu nhanh chóng dẫn đến hiện tượng tràn bộ nhớ (**Out-of-Memory — OOM Crash**) hoặc làm tê liệt hệ thống.
2. **Nghẽn tính toán trong tìm kiếm trạng thái tương đồng:** Bài toán tìm kiếm các mẫu kẹt xe lịch sử tương đồng với trạng thái giao thông hiện tại là bài toán cốt lõi trong giám sát và dự báo ITS. Tuy nhiên, thuật toán tìm kiếm láng giềng gần nhất chính xác (**Exact K-Nearest Neighbors — KNN**) có độ phức tạp thời gian $O(N \cdot d)$ cho mỗi truy vấn (trong đó $N$ là số lượng vector lịch sử và $d$ là số chiều của vector cửa sổ trượt không–thời gian). Khi $N$ đạt quy mô hàng triệu vector, độ trễ truy vấn tăng theo cấp số nhân, hoàn toàn không đáp ứng được yêu cầu phản hồi thời gian thực tính bằng mili-giây.
3. **Nhiễu loạn dữ liệu cảm biến và nguyên lý GIGO:** Dữ liệu cảm biến thực tế luôn chứa đựng một lượng lớn giá trị rác: cảm biến mất nguồn hoặc mất kết nối trả về giá trị $0.0$, cảm biến hỏng hóc trả về `NaN`, hoặc nhiễu vật lý vượt ngưỡng vận tốc tối đa của phương tiện. Theo nguyên lý **GIGO (*Garbage In, Garbage Out*)**, nếu dữ liệu rác không được kiểm toán và xử lý bằng các mô hình toán học chuẩn xác, toàn bộ kết quả phân tích thống kê và mô hình máy học tiếp theo sẽ hoàn toàn sai lệch.
4. **Khoảng cách giữa nghiên cứu chuẩn quốc tế và bài toán thực tế tại Việt Nam:** Phần lớn các nghiên cứu hiện nay chỉ dừng lại ở việc áp dụng thuật toán trên các tập dữ liệu đóng của Mỹ (`METR-LA`, `PEMS-BAY`) mà thiếu vắng sự đối chứng, liên hệ thực tế với cấu trúc mạng lưới giao thông đô thị phức tạp tại Việt Nam — nơi có mật độ nút giao dày đặc và chịu ảnh hưởng trực tiếp của yếu tố thời tiết mưa ngập nhiệt đới.

Xuất phát từ những đòi hỏi cấp bách trên, đề tài: **"Xây dựng pipeline xử lý dữ liệu lớn mạng lưới giao thông trên cấu trúc đồ thị không–thời gian bằng Apache Spark và kỹ thuật tìm kiếm láng giềng gần đúng (LSH/ANN): Thực nghiệm đối chứng trên METR-LA/PEMS-BAY và ứng dụng phân tích tại TP.HCM kết hợp thời tiết"** được lựa chọn nghiên cứu nhằm giải quyết trọn vẹn từ hạ tầng lưu trữ phân tán, kỹ thuật tối ưu hóa truy vấn đến kiểm chứng ứng dụng thực tiễn.

## 2. Mục tiêu nghiên cứu
Đề tài hướng tới việc thực hiện các mục tiêu cụ thể sau:
* **Mục tiêu 1 — Thiết kế Pipeline Phân tán & Lưu trữ Cột:** Xây dựng quy trình xử lý dữ liệu lớn khép kín dựa trên **Apache Spark (PySpark)** kết hợp định dạng lưu trữ cột **Apache Parquet (Snappy)** có cơ chế phân vùng theo tháng (`year_month`), giúp tối ưu hóa dung lượng đĩa cứng từ $3\times$ đến $7\times$ và hỗ trợ truy vấn song song tốc độ cao.
* **Mục tiêu 2 — Chuẩn hóa Kiểm toán & Tiền xử lý GIGO:** Xây dựng mô đun kiểm toán dữ liệu rác tự động, áp dụng các kỹ thuật toán học như nội suy cục bộ (*Time-series Interpolation*), lọc ngoại lai (*Interquartile Range — IQR*) và chuẩn hóa thang đo trên quy mô lớn bằng PySpark.
* **Mục tiêu 3 — Đo kiểm & Đối chứng Hiệu năng Toàn diện (Pandas vs. PySpark):** Xây dựng bộ công cụ `benchmark_profiler.py` để đo đạc khách quan và định lượng hai chỉ số tài nguyên sống còn: **Thời gian thực thi (Wall-clock Time tính bằng giây)** và **Bộ nhớ đỉnh (Peak RAM tính bằng MB)** trên các lát cắt dữ liệu quy mô từ $1\text{M} \to 5\text{M} \to 10\text{M} \to 17\text{M}$ bản ghi, chứng minh ưu thế của điện toán phân tán trên bộ nhớ chính.
* **Mục tiêu 4 — Khắc phục Nút thắt Tìm kiếm Tương đồng (KNN vs. LSH vs. ANN FAISS):** Hiện thực hóa và đối chứng thực nghiệm thuật toán tìm kiếm láng giềng gần đúng dựa trên băm ngẫu nhiên (**PySpark BucketedRandomProjectionLSH**) và cấu trúc đồ thị phân cấp / lượng tử hóa (**FAISS IndexFlatL2, IndexHNSWFlat**); đánh giá đa chiều sự đánh đổi giữa **Độ chính xác (Recall@K)**, **Độ trễ truy vấn (Query Latency ms)** và **Bộ nhớ chỉ mục (Index RAM MB)**.
* **Mục tiêu 5 — Chuyển giao Thực tiễn Mạng lưới Đô thị TP.HCM:** Trích xuất mô hình đồ thị có hướng $G = (V, E, W)$ từ mạng lưới đường bộ OpenStreetMap khu vực nút giao trọng điểm Ngã tư Hàng Xanh (Bình Thạnh, TP.HCM); kết hợp chuỗi thời gian khí tượng thực từ API Open-Meteo để xây dựng bản đồ GIS trực quan phục vụ công tác điều phối giao thông đô thị.

## 3. Đối tượng và Phạm vi Nghiên cứu
* **Đối tượng nghiên cứu:** 
  * Cấu trúc dữ liệu chuỗi thời gian giao thông đa biến trên đồ thị không–thời gian.
  * Các kiến trúc tính toán phân tán (Apache Spark RDD, DataFrame DAG, Catalyst Optimizer, Tungsten Execution Engine).
  * Các thuật toán tìm kiếm láng giềng gần đúng đa chiều (LSH, HNSW, Product Quantization).
  * Mạng lưới đường bộ đô thị trích xuất bằng OSMnx và dữ liệu khí tượng Open-Meteo.
* **Phạm vi nghiên cứu:**
  * **Dữ liệu lõi đối chứng:** Hai bộ dữ liệu cảm biến giao thông đường bộ chuẩn quốc tế là `METR-LA` ($7.094.304$ dòng, $207$ cảm biến) và `PEMS-BAY` ($16.937.700$ dòng, $325$ cảm biến).
  * **Dữ liệu Case Study thực tiễn:** Khu vực Ngã tư Hàng Xanh (quận Bình Thạnh, TP.HCM) với bán kính $1.000\text{m}$, chọn lọc $25$ nút giao trọng điểm trên $6$ trục đường chính (Xô Viết Nghệ Tĩnh, Điện Biên Phủ, Đinh Bộ Lĩnh, Bạch Đằng, Nguyễn Gia Trí, Ung Văn Khiêm) cùng dữ liệu thời tiết $90$ ngày ($2.160$ giờ) từ API Open-Meteo.
  * **Môi trường thực nghiệm:** Máy trạm cá nhân CPU đa lõi (x86_64) và môi trường điện toán đám mây Google Colab (Spark 3.5.x, Python 3.12, Java 17).

## 4. Phương pháp Nghiên cứu
* **Phương pháp thu thập và tiền xử lý dữ liệu lớn:** Tải dữ liệu CSV từ Zenodo, trích xuất cấu trúc đồ thị bằng OSMnx, chuẩn hóa thành định dạng bảng dài (Long-format) và đóng gói vào định dạng Parquet có partition theo tháng.
* **Phương pháp thực nghiệm đối chứng định lượng (Benchmarking):** Sử dụng các module đo đếm độc lập (`time.perf_counter()`, `psutil`) để đo lường tài nguyên tiêu hao một cách cô lập và lặp lại nhiều lần để lấy giá trị trung bình thống kê.
* **Phương pháp tiếp cận toán học và lý thuyết đồ thị:** Sử dụng hàm nhân Gaussian để tính ma trận kề $W_{ij}$, hàm phân vị IQR để xác định ngưỡng ngoại lai, và lý thuyết băm nhạy cảm vị trí để giảm số chiều tìm kiếm.

---

# CHƯƠNG 1. TỔNG QUAN BÀI TOÁN & CÁC CÔNG NGHỆ LIÊN QUAN

## 1.1. Bối cảnh Giao thông Thông minh & Đặc trưng 5V Big Data
Trong các đô thị hiện đại, giao thông đường bộ không chỉ đơn thuần là việc di chuyển vật lý của phương tiện mà đã trở thành một hệ thống thông tin phức hợp. Việc ứng dụng công nghệ thông tin và cảm biến vào giao thông hình thành nên Hệ thống Giao thông Thông minh (ITS), với mục tiêu cốt lõi là tối ưu hóa năng lực thông hành, giảm thiểu tai nạn và phát hiện sớm các nguy cơ ùn tắc. 

Dữ liệu cảm biến giao thông thu thập được mang đầy đủ 5 đặc trưng kinh điển của Dữ liệu lớn (**5V Big Data**):

```text
               ┌────────────────────────────────────────────────────────┐
               │        ĐẶC TRƯNG 5V CỦA DỮ LIỆU LỚN GIAO THÔNG          │
               └───────────────────────────┬────────────────────────────┘
                                           │
         ┌───────────────────┬─────────────┴───────┬───────────────────┐
         ▼                   ▼                     ▼                   ▼
   [ 1. VOLUME ]       [ 2. VELOCITY ]       [ 3. VARIETY ]      [ 4. VERACITY ]
 • METR-LA: 7.1M dòng • Tần suất: 5 phút/lần • Chuỗi thời gian   • 8.11% zero lỗi
 • PEMS-BAY: 16.9M    • Cửa sổ trượt trơn    • Đồ thị G=(V,E,W)  • Dữ liệu khuyết NaN
 • Parquet: 9.8-25MB  • Latency < ms         • JSON Open-Meteo   • Nguyên lý GIGO
         │                   │                     │                   │
         └───────────────────┴─────────────┬───────┴───────────────────┘
                                           │
                                           ▼
                                    [ 5. VALUE ]
                   • Giải quyết triệt để tràn RAM đơn máy (OOM)
                   • Tăng tốc tìm kiếm tương đồng: O(N) -> O(1)
                   • Ứng dụng thực tế: Case Study Hàng Xanh TP.HCM
```

* **Volume (Dung lượng lớn):**  
  Số lượng quan trắc giao thông tích lũy tăng trưởng theo cấp số nhân. Trong đề tài này, hai tập dữ liệu benchmark đại diện cho quy mô dữ liệu lớn chuẩn mực:
  * `METR-LA`: Bao gồm $207$ cảm biến đo liên tục trong $4$ tháng ($34.272$ mốc thời gian), tạo ra tổng cộng $7.094.304$ điểm dữ liệu đo lường.
  * `PEMS-BAY`: Bao gồm $325$ cảm biến tại khu vực Vịnh San Francisco đo liên tục trong $6$ tháng ($52.116$ mốc thời gian), tạo ra tổng cộng $16.937.700$ bản ghi.
  Quy mô từ $7\text{M}$ đến $17\text{M}$ dòng là ngưỡng dữ liệu vượt quá khả năng xử lý mượt mà của bộ nhớ RAM đơn máy thông thường khi thực hiện các phép toán phức tạp (như ma trận tương quan, gom cụm hoặc trượt cửa sổ thời gian đa biến).
* **Velocity (Tốc độ cập nhật & Xử lý thời gian thực):**  
  Dữ liệu từ các trạm quan sát được truyền về trung tâm điều khiển theo chu kỳ cố định $5$ phút/lần. Tốc độ này đòi hỏi hệ thống phân tích không chỉ thực hiện xử lý theo lô (Batch Processing) vào cuối ngày mà còn phải có khả năng xử lý cửa sổ trượt (*Sliding Window*) với độ trễ phản hồi cực thấp (dưới $100\text{ ms}$) để phục vụ việc cảnh báo tắc đường tức thì.
* **Variety (Sự đa dạng về nguồn & Cấu trúc dữ liệu):**  
  Hệ thống giao thông thông minh là sự kết hợp đa dạng của nhiều định dạng dữ liệu khác nhau:
  * Dữ liệu bảng chuỗi thời gian đa biến (*Multivariate Time-Series CSV / Parquet*): Lưu trữ vận tốc và lưu lượng theo thời gian.
  * Dữ liệu cấu trúc đồ thị không gian $G = (V, E, W)$: Thể hiện tính liên thông và khoảng cách địa lý giữa các nút cảm biến trên đường bộ (lưu dưới dạng ma trận kề $W$, file `.graphml` hoặc danh sách đỉnh/cạnh).
  * Dữ liệu thời tiết bán cấu trúc (REST API JSON): Dữ liệu khí tượng từ Open-Meteo bao gồm lượng mưa, nhiệt độ, độ ẩm và tốc độ gió.
* **Veracity (Tính chân thực & Nguyên lý GIGO):**  
  Do hoạt động ngoài trời trong điều kiện khắc nghiệt, các cảm biến phần cứng thường xuyên gặp sự cố: mất nguồn, đứt cáp quang truyền dẫn, hoặc bị xe tải che khuất. Kiểm toán thực tế của nhóm trên `METR-LA` ghi nhận tới $575.302$ ô mang giá trị $0.0$ (chiếm $8.11\%$ tổng số quan sát), đây là tín hiệu mất kết nối cảm biến chứ không phải vận tốc thực tế của dòng xe. Nếu áp dụng thuật toán trực tiếp mà không kiểm toán và xử lý (*Garbage In, Garbage Out*), các giá trị $0$ nhân tạo này sẽ làm sai lệch hoàn toàn khoảng cách Euclidean và trọng số đồ thị.
* **Value (Giá trị thực tiễn và Khoa học):**  
  Giá trị mang lại của hệ thống là khả năng khai phóng sức mạnh của điện toán phân tán Apache Spark, giúp các nhà quản lý giao thông dễ dàng truy vấn các hình thái ùn tắc lịch sử tương đồng thông qua LSH/ANN, từ đó đưa ra các kịch bản phân luồng chính xác, giảm thiểu thời gian chờ đợi cho người dân đô thị.

## 1.2. Khảo sát Nghiên cứu & Công nghệ Liên quan (Trích dẫn Chuẩn IEEE/ACM)
Nhằm xây dựng nền tảng lý luận vững chắc, đề tài tiến hành khảo sát $5$ công trình học thuật và công nghệ tiêu biểu đại diện cho $4$ trụ cột kiến trúc của hệ thống:

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    BẢN ĐỒ CÔNG NGHỆ VÀ HỌC THUẬT NỀN TẢNG                       │
├──────────────────────────────────────┬──────────────────────────────────────────┤
│ 1. DỮ LIỆU & ĐỒ THỊ KHÔNG–THỜI GIAN  │ 2. ĐIỆN TOÁN PHÂN TÁN BỘ NHỚ CHÍNH       │
│    • Li et al. (ICLR 2018): DCRNN    │    • Zaharia et al. (ACM 2016): Spark    │
│    • METR-LA & PEMS-BAY Benchmarks   │    • RDD, Catalyst Optimizer & Parquet   │
├──────────────────────────────────────┼──────────────────────────────────────────┤
│ 3. TRUY VẤN LÁNG GIỀNG GẦN ĐÚNG LỚN  │ 4. MÔ HÌNH HÓA ĐỒ THỊ ĐÔ THỊ THỰC TẾ     │
│    • Indyk & Motwani (STOC 1998): LSH│    • Boeing (CEUS 2017): OSMnx           │
│    • Johnson et al. (IEEE 2019): FAISS│    • Đồ thị Hàng Xanh + API Open-Meteo   │
└──────────────────────────────────────┴──────────────────────────────────────────┘
```

1. **Chuẩn hóa dữ liệu đồ thị giao thông — Li et al. (2018) [1]:**  
   Trong công trình *Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting* xuất bản tại ICLR 2018, nhóm tác giả Yaguang Li và cộng sự đã đặt nền móng cho việc biểu diễn mạng lưới đường bộ thành đồ thị có hướng $G = (V, E, W)$. Công trình đã công bố công khai hai bộ dữ liệu thực tế được chuẩn hóa cao độ: `METR-LA` và `PEMS-BAY`. Trọng số liên thông giữa hai cảm biến được tính bằng hàm nhân Gaussian dựa trên khoảng cách ngắn nhất trên mạng lưới đường bộ. Đề tài kế thừa cấu trúc đồ thị chuẩn này để thực hiện bài toán biểu diễn vector không gian.
2. **Điện toán phân tán trong bộ nhớ — Zaharia et al. (2016) [2]:**  
   Công trình kinh điển *Apache Spark: A Unified Engine for Big Data Processing* trên tạp chí Communications of the ACM đã chứng minh tính ưu việt của mô hình tập dữ liệu phân tán có khả năng phục hồi (RDD) và cơ chế thực thi lười biếng (Lazy Evaluation) thông qua đồ thị DAG. Spark cho phép lưu trữ trung gian trực tiếp trên RAM, khắc phục triệt để nhược điểm ghi đĩa liên tục của Hadoop MapReduce. Đề tài ứng dụng PySpark 3.5 và bộ tối ưu hóa Catalyst để xử lý song song các tác vụ nạp, lọc GIGO và chuyển đổi Parquet.
3. **Tìm kiếm láng giềng gần đúng bằng hàm băm — Indyk & Motwani (1998) [3]:**  
   Trong hội nghị STOC 1998, Indyk và Motwani đã giới thiệu kỹ thuật băm nhạy cảm vị trí (Locality-Sensitive Hashing — LSH). Bằng cách thiết kế các hàm băm sao cho các điểm gần nhau trong không gian đa chiều có xác suất rơi vào cùng một thùng băm cao, LSH giúp giảm độ phức tạp tìm kiếm từ $O(N \cdot d)$ xuống $O(d \cdot N^{1/(1+\epsilon)})$. Đề tài ứng dụng thuật toán `BucketedRandomProjectionLSH` của Spark MLlib để gom cụm các cửa sổ trượt giao thông tương đồng.
4. **Tìm kiếm tương đồng vector quy mô tỷ phần tử — Johnson et al. (2019) [4]:**  
   Công trình *Billion-scale similarity search with GPUs* trên IEEE Transactions on Big Data giới thiệu thư viện FAISS. Bằng cách khai thác tối đa tập lệnh phần cứng SIMD (AVX2) và cấu trúc đồ thị phân cấp HNSW (*Hierarchical Navigable Small World*), FAISS cho phép tra cứu vector với độ trễ chỉ vài micro-giây. Đề tài sử dụng FAISS làm chuẩn đối chứng vàng (*Ground Truth Benchmark*) để so sánh với thuật toán LSH phân tán của Spark.
5. **Mô hình hóa đồ thị đô thị từ OpenStreetMap — Boeing (2017) [5]:**  
   Geoff Boeing phát triển thư viện OSMnx, cho phép tải và dựng đồ thị đường bộ đô thị với đầy đủ thuộc tính topo và hình học. Đề tài sử dụng OSMnx để trích xuất mạng lưới đường bộ cụm Hàng Xanh (Bình Thạnh, TP.HCM), tạo tiền đề cho việc xây dựng Dashboard GIS trực quan hóa.

---

*Bảng 1.1: Bảng tổng hợp đối chiếu các công trình nghiên cứu liên quan*

| Tiêu chí đối chiếu | Li et al. (2018) [1] | Zaharia et al. (2016) [2] | Indyk & Motwani (1998) [3] | Johnson et al. (2019) [4] | Boeing (2017) [5] | **Đề tài Nhóm 11 (2026)** |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Trọng tâm nghiên cứu** | Dự báo giao thông bằng DCRNN | Hạ tầng tính toán phân tán Spark | Lý thuyết băm đa chiều LSH | Chỉ mục vector cực nhanh FAISS | Đồ thị đường bộ OSMnx | **Pipeline Big Data + LSH/ANN + TP.HCM** |
| **Quy mô dữ liệu** | $7\text{M} - 17\text{M}$ dòng | Petabytes | Vector đa chiều | Hàng tỷ vector | Đồ thị thành phố | **$1\text{M} \to 17\text{M}$ dòng & 25 nút HCM** |
| **Cấu trúc lưu trữ** | File HDF5 / Pickle | RDD / Parquet | Bộ nhớ RAM | Inverted Index / HNSW | NetworkX GraphML | **Parquet có partition theo tháng** |
| **Xử lý phân tán** | Không (GPU máy đơn) | Có (Cụm phân tán) | Không | Không (Đơn node CPU/GPU) | Không | **Apache Spark (PySpark đa lõi)** |
| **Tìm kiếm tương đồng** | Không | Không | LSH toán học | IVF-PQ, HNSW | Không | **Đối chứng KNN vs. LSH vs. FAISS** |
| **Ứng dụng tại VN** | Không (Dữ liệu Mỹ) | Không | Không | Không | Không | **Case Study Ngã tư Hàng Xanh** |

---

## 1.3. Khoảng trống Nghiên cứu (Research Gap) & Mục tiêu Đề tài
Từ việc phân tích tổng hợp các công trình trên, đề tài chỉ ra $3$ khoảng trống nghiên cứu chính cần lấp đầy:

1. **Khoảng trống 1 — Giới hạn mở rộng và tràn RAM của các công cụ đơn máy:**  
   Hầu hết các nhà nghiên cứu khi tiếp cận bộ dữ liệu `METR-LA` hay `PEMS-BAY` đều nạp trực tiếp vào RAM bằng Pandas để huấn luyện mô hình học sâu. Cách làm này bỏ qua bài toán xử lý dữ liệu lớn ở tầng tiền xử lý, dẫn đến sự lãng phí tài nguyên và không thể mở rộng khi dữ liệu tăng trưởng liên tục.  
   *Giải pháp của đề tài:* Thiết lập quy chuẩn lưu trữ cột **Parquet nén Snappy**, phân vùng theo `year_month`, giúp giảm dung lượng từ $69.4\text{ MB}$ xuống $9.8\text{ MB}$ (giảm $7.1\times$ cho METR-LA) và từ $81.7\text{ MB}$ xuống $25.4\text{ MB}$ (giảm $3.2\times$ cho PEMS-BAY), tối ưu hóa quá trình đọc chọn lọc thông qua *predicate pushdown*.
2. **Khoảng trống 2 — Nút thắt thời gian tính toán của KNN vét cạn trong ITS:**  
   Để tìm kiếm hình thái kẹt xe tương đồng, việc quét toàn bộ $N$ vector lịch sử ($O(N \cdot d)$) hoàn toàn bất khả thi cho các ứng dụng ITS trực tuyến. Tuy nhiên, việc áp dụng LSH phân tán trên Spark hay FAISS trên máy trạm chưa được so sánh định lượng một cách có hệ thống về trade-off giữa độ chính xác và tài nguyên.  
   *Giải pháp của đề tài:* Thực hiện đối chứng định lượng toàn diện giữa **Exact KNN**, **PySpark LSH** và **FAISS HNSW** trên các tập vector mẫu từ $10.000$ đến hàng triệu vector, cung cấp bảng số liệu thực nghiệm đo đạc `Recall@10`, `Query Latency` và `Peak RAM`.
3. **Khoảng trống 3 — Thiếu tính thực chứng và chuyển giao trên giao thông đô thị Việt Nam:**  
   Các bài toán nghiên cứu giao thông dữ liệu lớn thường mang tính lý thuyết hoặc chỉ chạy trên dữ liệu nước ngoài. TP.HCM có đặc thù giao thông xe máy chiếm đa số, mật độ đường ngang dày đặc và thường xuyên chịu ảnh hưởng của triều cường và mưa ngập.  
   *Giải pháp của đề tài:* Xây dựng Case Study trích xuất trực tiếp $25$ nút giao trọng điểm Ngã tư Hàng Xanh, kết hợp dữ liệu khí tượng Open-Meteo và tuân thủ nguyên tắc khoa học nghiêm ngặt: **chỉ phân tích bối cảnh không gian GIS, tuyệt đối không hồi quy OLS chéo địa lý giữa thời tiết TP.HCM và vận tốc xe tại Mỹ**.

## 1.4. Bố cục Quyển Báo cáo
Quyển báo cáo đồ án được kết cấu chặt chẽ thành $3$ chương chính và phần kết luận, bám sát rubric quy định ($\ge 40$ trang):
* **PHẦN MỞ ĐẦU:** Trình bày bối cảnh nghiên cứu, tính cấp thiết, mục tiêu, đối tượng, phạm vi và phương pháp nghiên cứu của đồ án.
* **CHƯƠNG 1. TỔNG QUAN BÀI TOÁN & CÁC CÔNG NGHỆ LIÊN QUAN:** Trình bày chi tiết bối cảnh giao thông thông minh, phân tích đặc thù 5V Big Data, khảo sát 5 công trình học thuật nền tảng chuẩn IEEE/ACM và xác lập 3 khoảng trống nghiên cứu (Research Gap).
* **CHƯƠNG 2. PHƯƠNG PHÁP ÁP DỤNG CHO BÀI TOÁN ($> 13$ trang):** Trình bày chuyên sâu cơ sở toán học và kiến trúc kỹ thuật: Kiến trúc Apache Spark (RDD, Catalyst, Parquet), nền tảng toán học của kiểm toán GIGO và hồi quy OLS, lý thuyết biểu diễn đồ thị không–thời gian, toán học thuật toán LSH (Random Projection) và kiến trúc chỉ mục vector FAISS (IVF-PQ, HNSW).
* **CHƯƠNG 3. TRIỂN KHAI HỆ THỐNG, THỰC NGHIỆM & ĐÁNH GIÁ ($> 13$ trang):** Mô tả chi tiết hạ tầng triển khai thực tế; Kịch bản 1: Kiểm toán và làm sạch GIGO trên PySpark; Kịch bản 2: Thực nghiệm đối chứng hiệu năng tính toán tài nguyên (Pandas vs. PySpark qua các mốc $1\text{M} \to 17\text{M}$ dòng); Kịch bản 3: Đối chứng hiệu năng tìm kiếm tương đồng (Exact KNN vs. PySpark LSH vs. FAISS); Kịch bản 4: Case Study trực quan hóa GIS bản đồ Hàng Xanh kết hợp thời tiết Open-Meteo.
* **KẾT LUẬN & HƯỚNG PHÁT TRIỂN:** Tổng kết các đóng góp nổi bật của đồ án, đánh giá mức độ hoàn thành so với mục tiêu ban đầu, thẳng thắn chỉ ra các hạn chế còn tồn đọng và đề xuất các hướng mở rộng tiếp theo.
* **TÀI LIỆU THAM KHẢO:** Danh mục tài liệu tham khảo chuẩn IEEE/ACM được trích dẫn đầy đủ và chuẩn xác trong báo cáo.
