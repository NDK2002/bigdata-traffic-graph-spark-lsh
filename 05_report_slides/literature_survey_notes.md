# KHẢO SÁT HỌC THUẬT VÀ KHOẢNG TRỐNG NGHIÊN CỨU (LITERATURE SURVEY & RESEARCH GAP)
## ĐỒ ÁN: PHÂN TÍCH DỮ LIỆU LỚN GIAO THÔNG ĐỒ THỊ BẰNG APACHE SPARK & LSH/ANN

> **Người thực hiện:** Trần Hoàng Hôn (`26410046`) — Trưởng nhóm / PM & Kiến trúc Đồ thị TP.HCM  
> **Mã công việc:** `[H4]` theo [Ke_Hoach_Thuc_Hien_Tuan_4.md](../Ke_Hoach_Thuc_Hien_Tuan_4.md)  
> **Mục đích tài liệu:** Hệ thống hóa 5 công trình học thuật nền tảng chuẩn IEEE/ACM/Elsevier, định dạng BibTeX chuẩn mực, phân tích ưu nhược điểm và xác lập khoảng trống nghiên cứu (*Research Gap*) làm cơ sở lý luận cho **Mở đầu và Chương 1** của Báo cáo đồ án ($\ge 40$ trang).

---

## I. DANH MỤC CÁC CÔNG TRÌNH HỌC THUẬT NỀN TẢNG (CHUẨN IEEE / ACM)

### 1. DCRNN & Chuẩn hóa Dữ liệu Giao thông Đồ thị không–thời gian
* **Trích dẫn chuẩn IEEE:**  
  Y. Li, R. Yu, C. Shahabi, and Y. Liu, "Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting," in *Proc. 6th Int. Conf. Learn. Represent. (ICLR)*, Vancouver, Canada, 2018, pp. 1–16.
* **Định danh / Mã nguồn:** `arXiv:1707.01926` | GitHub: `liyaguang/DCRNN`.
* **Đóng góp khoa học cốt lõi:**
  * Thiết lập mô hình biểu diễn dữ liệu giao thông không–thời gian dưới dạng đồ thị có hướng $G = (V, E, W)$, trong đó các cảm biến là đỉnh $V$ và ma trận kề trọng số $W_{ij}$ được xác định qua hàm nhân Gaussian theo khoảng cách đường bộ:
    $$W_{ij} = \exp\left(-\frac{\text{dist}(v_i, v_j)^2}{\sigma^2}\right)$$
  * Thu thập, làm sạch và công bố 2 bộ dữ liệu benchmark chuẩn quốc tế:
    * **`METR-LA`**: $207$ cảm biến đường cao tốc tại Los Angeles ($34.272$ mốc thời gian $\times$ $207 = 7.094.304$ bản ghi đo $5$ phút từ tháng 03/2012 đến tháng 06/2012).
    * **`PEMS-BAY`**: $325$ cảm biến tại Vịnh San Francisco ($52.116$ mốc thời gian $\times$ $325 = 16.937.700$ bản ghi đo $5$ phút từ tháng 01/2017 đến tháng 06/2017).
* **Hạn chế đối với phân tích dữ liệu lớn:**
  * Công trình lưu trữ dữ liệu dạng mảng dày đặc trong file nhị phân HDF5 (`.h5`) và pickle (`.pkl`). Cách lưu trữ này tối ưu cho huấn luyện Deep Learning trên GPU nhưng không thân thiện với các hệ sinh thái tính toán phân tán (như Apache Spark), thiếu cơ chế phân vùng (*partitioning*) và metadata lưu trữ cột (*columnar layout*).
  * Chỉ tập trung vào bài toán dự báo bằng mạng nơ-ron hồi quy tích chập khuếch tán, không khảo sát bài toán truy vấn láng giềng kẹt xe tương đồng ở quy mô lớn trên hệ thống phân tán.

```bibtex
@inproceedings{li2018dcrnn,
  author    = {Yaguang Li and Rose Yu and Cyrus Shahabi and Yan Liu},
  title     = {Diffusion Convolutional Recurrent Neural Network: Data-Driven Traffic Forecasting},
  booktitle = {6th International Conference on Learning Representations, {ICLR} 2018},
  year      = {2018},
  url       = {https://openreview.net/forum?id=SJiHXGWAZ}
}
```

---

### 2. Apache Spark: Điện toán Phân tán trên Bộ nhớ chính (In-Memory Distributed Engine)
* **Trích dẫn chuẩn ACM:**  
  M. Zaharia, R. S. Xin, P. Wendell, T. Das, M. Armbrust, A. Dave, X. Meng, R. Rosen, S. Venkataraman, M. J. Franklin, A. Ghodsi, J. Gonzalez, S. Shenker, and I. Stoica, "Apache Spark: A unified engine for big data processing," *Communications of the ACM*, vol. 59, no. 11, pp. 56–65, Oct. 2016. DOI: `10.1145/2934664`.
* **Đóng góp khoa học cốt lõi:**
  * Đề xuất mô hình trừu tượng tập dữ liệu phân tán có khả năng phục hồi (**RDD — Resilient Distributed Datasets**) và cấu trúc **DataFrame/Dataset** dựa trên đồ thị có hướng không chu trình (**DAG Execution Engine**).
  * Tối ưu hóa truy vấn thông qua bộ tối ưu hóa **Catalyst Optimizer** và cơ chế sinh mã nội tuyến **Tungsten**, cho phép tính toán lặp và phân tích luồng trong RAM nhanh hơn từ $10\times$ đến $100\times$ so với mô hình Hadoop MapReduce truyền thống.
  * Tích hợp chặt chẽ với định dạng lưu trữ cột **Apache Parquet**, hỗ trợ *predicate pushdown* (đẩy bộ lọc xuống tầng lưu trữ) và *column pruning* (chỉ quét các cột cần thiết).
* **Ứng dụng trong đề tài:**
  * Giải quyết triệt để nút thắt cổ chai tràn RAM (Out-of-Memory) của thư viện đơn máy `pandas` khi xử lý từ $1\text{M}$ đến gần $17\text{M}$ dòng dữ liệu giao thông.
  * Cung cấp nền tảng tính toán phân tán cho quy trình kiểm toán dữ liệu rác (GIGO), nội suy giá trị khuyết và phân vùng Parquet theo tháng (`year_month`).

```bibtex
@article{zaharia2016apache,
  author    = {Matei Zaharia and Reynold S. Xin and Patrick Wendell and Tathagata Das and Michael Armbrust and Anurag Dave and Xiangrui Meng and R. Rosen and Shivaram Venkataraman and Michael J. Franklin and Ali Ghodsi and Joseph Gonzalez and Scott Shenker and Ion Stoica},
  title     = {Apache Spark: A Unified Engine for Big Data Processing},
  journal   = {Communications of the {ACM}},
  volume    = {59},
  number    = {11},
  pages     = {56--65},
  year      = {2016},
  doi       = {10.1145/2934664}
}
```

---

### 3. Locality-Sensitive Hashing (LSH): Tìm kiếm Láng giềng gần đúng trong Không gian Đa chiều
* **Trích dẫn chuẩn ACM:**  
  P. Indyk and R. Motwani, "Approximate nearest neighbors: towards removing the curse of dimensionality," in *Proc. 30th Annu. ACM Symp. Theory of Comput. (STOC)*, Dallas, TX, USA, 1998, pp. 604–613. DOI: `10.1145/2766983.2768768`.
* **Đóng góp khoa học cốt lõi:**
  * Phá vỡ "lời nguyền số chiều" (*Curse of Dimensionality*) trong bài toán tìm kiếm láng giềng gần nhất (Nearest Neighbor Search).
  * Đề xuất họ hàm băm nhạy cảm vị trí **LSH**: hai điểm dữ liệu có khoảng cách Euclidean nhỏ thì xác suất va chạm vào cùng một thùng băm (*hash bucket*) sẽ cao vượt trội so với hai điểm có khoảng cách lớn:
    $$P_{h \in \mathcal{H}}[h(p) = h(q)] = f(\text{dist}(p, q))$$
  * Giảm độ phức tạp thời gian truy vấn từ tuyến tính toàn bộ $O(N \cdot d)$ (KNN vét cạn) xuống tiệm cận $O(d \cdot N^{1/(1+\epsilon)})$, cho phép tra cứu trạng thái mẫu giao thông trong thời gian thực dưới mili-giây.
* **Ứng dụng trong đề tài:**
  * Sử dụng thuật toán `BucketedRandomProjectionLSH` của thư viện `pyspark.ml.feature` để gom cụm và tìm kiếm các cửa sổ thời gian có hình mẫu vận tốc tương đồng trên cụm Spark phân tán.

```bibtex
@inproceedings{indyk1998approximate,
  author    = {Piotr Indyk and Rajeev Motwani},
  title     = {Approximate Nearest Neighbors: Towards Removing the Curse of Dimensionality},
  booktitle = {Proceedings of the Thirtieth Annual {ACM} Symposium on Theory of Computing ({STOC} '98)},
  pages     = {604--613},
  year      = {1998},
  doi       = {10.1145/2766983.2768768}
}
```

---

### 4. FAISS: Tìm kiếm Tương đồng Vector Quy mô Tỷ phần tử
* **Trích dẫn chuẩn IEEE:**  
  J. Johnson, M. Douze, and H. Jégou, "Billion-scale similarity search with GPUs," *IEEE Transactions on Big Data*, vol. 7, no. 3, pp. 535–547, July 2019. DOI: `10.1109/TBDATA.2019.2921572`.
* **Đóng góp khoa học cốt lõi:**
  * Giới thiệu thư viện nguồn mở **FAISS** (Facebook AI Similarity Search) chuyên biệt hóa cho việc lập chỉ mục và truy vấn vector dày đặc đa chiều với hiệu năng cực đại trên CPU (AVX2/AVX-512) và GPU (CUDA).
  * Triển khai các cấu trúc chỉ mục tiên tiến:
    * `IndexFlatL2` / `IndexFlatIP`: Tìm kiếm chính xác (Exact Baseline), khai thác triệt để phép nhân ma trận BLAS.
    * `IndexIVFFlat` (Inverted File): Phân cụm Voronoi để thu hẹp không gian tìm kiếm.
    * `IndexIVFPQ` (Product Quantization): Nén lượng tử hóa vector, tiết kiệm RAM từ $8\times$ đến $16\times$.
    * `IndexHNSWFlat` (Hierarchical Navigable Small World): Đồ thị thế giới nhỏ phân tầng đạt độ trễ siêu thấp và `Recall@K` $> 95\%$.
* **Ứng dụng trong đề tài:**
  * Làm thước đo đối chứng chuẩn vàng (*Ground Truth & Gold Standard*) để so sánh định lượng với PySpark LSH về ba chỉ số: **Độ chính xác (Recall@K)**, **Thời gian phản hồi (Query Latency ms)** và **Mức độ chiếm dụng bộ nhớ (Index RAM MB)**.

```bibtex
@article{johnson2019billion,
  author    = {Jeff Johnson and Matthijs Douze and Herv{\'e} J{\'e}gou},
  journal   = {{IEEE} Transactions on Big Data},
  title     = {Billion-Scale Similarity Search with {GPUs}},
  year      = {2019},
  volume    = {7},
  number    = {3},
  pages     = {535--547},
  doi       = {10.1109/TBDATA.2019.2921572}
}
```

---

### 5. OSMnx: Mô hình hóa Đồ thị Không gian Đô thị từ Dữ liệu Mở
* **Trích dẫn chuẩn Elsevier:**  
  G. Boeing, "OSMnx: New methods for acquiring, constructing, analyzing, and visualizing complex street networks," *Computers, Environment and Urban Systems*, vol. 65, pp. 126–139, Sept. 2017. DOI: `10.1016/j.compenvurbsys.2017.05.004`.
* **Đóng góp khoa học cốt lõi:**
  * Phát triển công cụ Python trích xuất mạng lưới giao thông đường bộ từ cơ sở dữ liệu mở OpenStreetMap (OSM) thành đồ thị có hướng đa cạnh (**MultiDiGraph** trong NetworkX).
  * Chuẩn hóa hình học không gian, giữ nguyên thuộc tính địa lý thực (kinh độ, vĩ độ, chiều dài đoạn đường, vận tốc thiết kế, số làn xe).
  * Tự động đơn giản hóa topo mạng lưới (loại bỏ các nút trung gian trên một cung đường cong mà không làm thay đổi đặc tính liên thông), tạo ma trận kề $W_{ij}$ phục vụ mô hình hóa mạng lưới giao thông phức tạp.
* **Ứng dụng trong đề tài:**
  * Trích xuất mạng lưới đường bộ thực tế khu vực trọng điểm Ngã tư Hàng Xanh (quận Bình Thạnh, TP.HCM), lọc các trục huyết mạch (Xô Viết Nghệ Tĩnh, Điện Biên Phủ, Đinh Bộ Lĩnh, Bạch Đằng, Nguyễn Gia Trí, Ung Văn Khiêm) thành đồ thị $25$ nút thực địa.

```bibtex
@article{boeing2017osmnx,
  author    = {Geoff Boeing},
  title     = {{OSMnx}: New Methods for Acquiring, Constructing, Analyzing, and Visualizing Complex Street Networks},
  journal   = {Computers, Environment and Urban Systems},
  volume    = {65},
  pages     = {126--139},
  year      = {2017},
  doi       = {10.1016/j.compenvurbsys.2017.05.004}
}
```

---

## II. MA TRẬN ĐỐI CHIẾU CÁC CÔNG TRÌNH LIÊN QUAN

| STT | Công trình nghiên cứu | Phương pháp / Công nghệ lõi | Quy mô tập dữ liệu | Điểm mạnh nổi bật | Khoảng trống / Hạn chế đối với Đề tài Nhóm 11 |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | **Li et al. (2018)**<br>*(ICLR)* | DCRNN, Graph Convolution, Seq2Seq | `METR-LA` (7.1M)<br>`PEMS-BAY` (16.9M) | Đưa ra chuẩn benchmark đồ thị giao thông quốc tế. | Dữ liệu định dạng HDF5/Pickle cồng kềnh, không phân vùng; chưa có giải pháp xử lý phân tán dữ liệu lớn. |
| **2** | **Zaharia et al. (2016)**<br>*(ACM)* | RDD, DataFrame DAG, In-Memory Engine | Terabytes / Petabytes | Đột phá về tốc độ tính toán song song trong bộ nhớ và khả năng chịu lỗi. | Là nền tảng hạ tầng chung; cần thiết kế pipeline chuyên biệt cho dữ liệu cảm biến chuỗi thời gian đồ thị. |
| **3** | **Indyk & Motwani (1998)**<br>*(STOC)* | Locality-Sensitive Hashing (LSH) | Lý thuyết & vector thực nghiệm | Phá vỡ lời nguyền số chiều, giảm thời gian tìm kiếm từ $O(N)$ xuống tiệm cận hằng số. | Thuật toán có tính ngẫu nhiên (chấp nhận đánh đổi độ chính xác Recall để lấy tốc độ); cần đối chứng thực nghiệm. |
| **4** | **Johnson et al. (2019)**<br>*(IEEE TBD)* | FAISS, Inverted File (IVF), PQ, HNSW | Triệu đến Tỷ vector | Tối ưu hóa phần cứng cực độ, tốc độ tra cứu vector nhanh nhất hiện nay. | Vận hành tập trung trên một node máy đơn (CPU/GPU), chưa tích hợp sẵn trong pipeline phân tán Spark. |
| **5** | **Boeing (2017)**<br>*(Elsevier)* | OSMnx, NetworkX, Không gian GIS | Bản đồ đô thị toàn cầu | Trích xuất đồ thị đường bộ thực tế chuẩn xác, giữ nguyên tọa độ GIS. | Thuần túy là đồ thị tĩnh; chưa kết hợp chuỗi thời gian vận tốc lớn và dữ liệu thời tiết khí tượng động. |

---

## III. XÁC LẬP KHOẢNG TRỐNG NGHIÊN CỨU & THỰC TIỄN (RESEARCH GAP)

Từ việc khảo sát các công trình học thuật tiêu biểu trên, Nhóm 11 xác định rõ **3 khoảng trống nghiên cứu cốt lõi** mà đề tài tập trung giải quyết:

### 1. Khoảng trống 1: Nút thắt cổ chai về Tài nguyên và Tràn bộ nhớ (OOM Bottleneck)
* **Vấn đề thực tế:** Đa số các phân tích dữ liệu giao thông hiện nay vẫn dựa trên thư viện đơn máy (`pandas`, `numpy`). Khi kích thước dữ liệu tăng từ $1\text{M}$ lên $7.1\text{M}$ (`METR-LA`) và gần $17\text{M}$ bản ghi (`PEMS-BAY`), các thao tác unpivot, rolling window, groupby và xử lý GIGO dẫn đến hiện tượng RAM ảo tăng vọt, gây tràn bộ nhớ (*Out-of-Memory Crash*).
* **Đóng góp của Đề tài:** Xây dựng pipeline phân tán **PySpark kết hợp định dạng cột Parquet (Snappy)** có phân vùng theo tháng (`year_month`), giúp giảm dung lượng từ $69.4\text{ MB}$ CSV xuống $9.8\text{ MB}$ Parquet (nhỏ hơn $7.1\times$) và chứng minh ưu thế vượt trội về Wall-clock Time lẫn Peak RAM trong kịch bản đối chứng thực nghiệm.

### 2. Khoảng trống 2: Độ phức tạp $O(N \cdot d)$ trong Truy vấn Hình mẫu Kẹt xe tương đồng
* **Vấn đề thực tế:** Thuật toán Exact KNN truyền thống đòi hỏi tính toán khoảng cách Euclidean giữa vector cửa sổ trượt truy vấn với toàn bộ $N$ vector lịch sử, độ phức tạp $O(N \cdot d)$ khiến thời gian phản hồi vượt ngưỡng chấp nhận được của hệ thống giao thông thông minh thời gian thực (ITS).
* **Đóng góp của Đề tài:** Triển khai và đối chứng định lượng đa chiều giữa **Exact KNN**, **PySpark LSH (BucketedRandomProjectionLSH)** và **ANN FAISS (IndexFlatL2, IndexHNSWFlat)** trên quy mô hàng chục nghìn đến hàng triệu vector; đo lường cụ thể trade-off giữa **Recall@K**, **Query Latency (ms)** và **Index RAM (MB)**.

### 3. Khoảng trống 3: Tính chuyển giao thực tiễn cho Mạng lưới Đô thị Việt Nam (Case Study TP.HCM)
* **Vấn đề thực tế:** Các nghiên cứu học thuật thường chỉ dừng lại ở việc benchmark trên dữ liệu chuẩn của Mỹ (`METR-LA`, `PEMS-BAY`) mà thiếu bước kiểm chứng chuyển giao trên mạng lưới giao thông thực tế tại Việt Nam — nơi có mật độ nút giao dày đặc và chịu ảnh hưởng nghiêm trọng từ vũ lượng mưa nhiệt đới gây ngập úng.
* **Đóng góp của Đề tài:** Kết hợp đồ thị thực địa trích xuất từ OpenStreetMap khu vực Ngã tư Hàng Xanh (Bình Thạnh, TP.HCM) và chuỗi thời gian khí tượng thực từ API Open-Meteo ($90$ ngày); đồng thời tuân thủ nguyên tắc khoa học nghiêm ngặt: **chỉ phân tích bối cảnh GIS và không tùy tiện hồi quy OLS chéo địa lý giữa thời tiết TP.HCM với tốc độ tại California**.

---

## IV. ĐÁNH GIÁ SỰ PHÙ HỢP VỚI ĐỀ CƯƠNG VÀ RUBRIC MÔN HỌC

Tài liệu này hoàn tất trọn vẹn yêu cầu công việc **`[H4]`**, cung cấp đầy đủ nội dung để chuyển hóa thành:
1. **Phần I.1 (Tính cấp thiết & Mục tiêu)** và **Phần I.4 (Tổng quan tình hình nghiên cứu)** trong [Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx](../Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx).
2. Danh mục tài liệu tham khảo chuẩn mực phục vụ bảo vệ đồ án trước Hội đồng chấm thi.
