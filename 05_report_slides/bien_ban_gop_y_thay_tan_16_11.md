# BIÊN BẢN GÓP Ý VÀ TRAO ĐỔI VỚI GIẢNG VIÊN HƯỚNG DẪN
## MÔN: PHÂN TÍCH DỮ LIỆU LỚN (`BIG DATA ANALYTICS`) — NHÓM 11 (CITD / UIT)

> **Thời gian:** `19:30 – 20:15`, Thứ Hai, ngày `16/11/2026`  
> **Địa điểm:** Phòng học môn Phân tích dữ liệu lớn / Trao đổi trực tiếp trên lớp  
> **Giảng viên hướng dẫn:** TS. Hà Minh Tân  
> **Người ghi biên bản:** Trần Hoàng Hôn (`26410046`) — Trưởng nhóm Nhóm 11  
> **Thành viên tham dự:** Đủ 4 thành viên (Trần Hoàng Hôn, Nguyễn Duy Khang, Phạm Thành Trung, Vũ Văn Duy)  
> **Tài liệu trình duyệt:** [De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx](../De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx) & Bảng đăng ký đề tài Hàng 33 [Final_Project.xlsx](../../Final_Project.xlsx).

---

## I. MỤC TIÊU PHIÊN LÀM VIỆC

1. Báo cáo tiến độ hoàn thành Tuần 4 và trình duyệt Đề cương chi tiết của Nhóm 11 theo đúng rubric `YeuCau` ($\ge 40$ trang).
2. Xin ý kiến chỉ đạo của TS. Hà Minh Tân về:
   - Phạm vi thực nghiệm đối chứng Dữ liệu lớn ($1\text{M} \to 17\text{M}$ dòng trên `METR-LA` và `PEMS-BAY`).
   - Hướng tiếp cận thuật toán tìm kiếm láng giềng gần đúng quy mô lớn (**KNN vét cạn** vs. **PySpark LSH** vs. **ANN FAISS**).
   - Quy tắc khoa học khi kết hợp Case Study mạng lưới đường bộ TP.HCM (Hàng Xanh) với dữ liệu thời tiết `Open-Meteo`.

---

## II. NỘI DUNG GÓP Ý VÀ KẾT LUẬN CỦA THẦY TÂN

### 1. Về Tên đề tài, Phạm vi và Đăng ký trên hệ thống
* **Kết luận GVHD:** 
  * Xác nhận thông qua đề tài của Nhóm 11 tại Hàng `33` file [Final_Project.xlsx](../../Final_Project.xlsx).
  * Đánh giá cao việc nhóm chủ động chọn hướng tiếp cận kết hợp dữ liệu lớn chuẩn quốc tế (`METR-LA`/`PEMS-BAY`) và có case study thực tiễn tại TP.HCM.
  * Đề tài đáp ứng đầy đủ tiêu chí môn học về tính toán phân tán, cấu trúc lưu trữ hiện đại và giải quyết trọn vẹn bài toán lớn.

### 2. Về Pipeline Apache Spark, Parquet và Kiểm toán Dữ liệu (GIGO)
* **Ý kiến Thầy Tân:**
  * Xử lý dữ liệu lớn trên Spark phải chú trọng cấu trúc lưu trữ: chuyển đổi CSV sang **Parquet có phân vùng (Partitioning)** là hướng đi chuẩn xác. Phải kiểm tra kỹ việc đọc/ghi tránh lỗi kiểu dữ liệu thời gian (`timestamp_ntz` / `microsecond`) giữa PySpark và PyArrow.
  * Nhấn mạnh nguyên lý **GIGO (*Garbage In, Garbage Out*)**: Cảm biến giao thông luôn có lỗi mất tín hiệu (trả về 0) hoặc mất kết nối (`NaN`). Nhóm bắt buộc phải có báo cáo kiểm toán số lượng ô lỗi và áp dụng phương pháp nội suy hoặc xử lý ngoại lai (IQR) trước khi đưa vào thuật toán phân tích.
* **Hành động của nhóm:** 
  * Khang (`[K2]`) đã hoàn thiện Data Contract chuẩn 6 cột và script phân vùng `year_month`.
  * Trung (`[T3]`) đã chạy xong báo cáo kiểm toán `gigo_audit_report.md` thống kê $8.11\%$ ô vận tốc $0.0$ của METR-LA.

### 3. Về Thực nghiệm Đối chứng Hiệu năng và Thuật toán (KNN vs. LSH vs. ANN)
* **Ý kiến Thầy Tân:**
  * Việc so sánh **Pandas đơn máy vs. PySpark phân tán** cần có kịch bản định lượng rõ ràng: đo lường **Wall-clock Time (giây)** và **Peak RAM (MB)** qua các mốc dữ liệu tăng dần từ $1\text{M} \to 5\text{M} \to 10\text{M} \to 17\text{M}$ dòng để chỉ ra điểm gãy (OOM) của Pandas.
  * Khi đối chứng **Exact KNN vs. LSH vs. FAISS**, ngoài độ trễ truy vấn (*Query Latency ms*), nhóm phải đo đạc chỉ số **Recall@K** để làm rõ sự đánh đổi (*trade-off*) giữa độ chính xác và tốc độ tính toán.
* **Hành động của nhóm:** 
  * Duy (`[D1]`) chuẩn bị module `benchmark_profiler.py` đo RAM và thời gian thực thi.
  * Khang (`[K3]`) thiết kế kịch bản smoke-test $10.000$ vector và thực nghiệm quy mô lớn ở Tuần 7.

### 4. Về Nguyên tắc Kết hợp Dữ liệu Thời tiết TP.HCM (Nguyên tắc OLS Chéo vùng)
* **Ý kiến chỉ đạo đặc biệt của Thầy Tân:**
  * **TUYỆT ĐỐI KHÔNG** lấy chuỗi thời tiết mưa ngập tại TP.HCM (Open-Meteo) để chạy hồi quy OLS với chuỗi tốc độ xe tại California (`METR-LA` / `PEMS-BAY`). Việc ghép nối chéo địa lý và chéo mốc thời gian là sai lệch phương pháp luận nghiên cứu dữ liệu.
  * Dữ liệu thời tiết TP.HCM và đồ thị Hàng Xanh (`OSMnx`) chỉ được dùng để minh họa **bối cảnh không gian GIS (Case Study TP.HCM)** trên Dashboard, hoặc nếu muốn phân tích tương quan mưa–vận tốc thì phải có dữ liệu giao thông nội địa cùng kỳ quan sát.
* **Hành động của nhóm:** 
  * Nhóm đã bổ sung quy định bắt buộc này vào tất cả các tài liệu (`README.md`, `data_sources.md`, `Ke_Hoach_Thuc_Hien_Tuan_4.md`).

### 5. Về Quy cách Báo cáo Đồ án ($\ge 40$ trang)
* **Kết luận GVHD:**
  * Báo cáo phải tuân thủ nghiêm ngặt cấu trúc: Mở đầu, Chương 1 (Tổng quan & Gap), Chương 2 (Cơ sở lý thuyết $> 13$ trang), Chương 3 (Thực nghiệm & Phân tích kết quả $> 13$ trang), Kết luận.
  * Tài liệu tham khảo phải trích dẫn chuẩn IEEE/ACM và có bảng đối chiếu Research Gap rõ ràng.

---

## III. PHÂN CÔNG NHIỆM VỤ TIẾP THEO (BƯỚC VÀO TUẦN 5 & 6)

1. **Trần Hoàng Hôn:** Hoàn thành khảo sát tài liệu học thuật `literature_survey_notes.md` (`[H4]`) và chấp bút viết toàn văn **Mở đầu & Chương 1** theo khung chuẩn.
2. **Nguyễn Duy Khang & Phạm Thành Trung:** Hoàn tất trọn vẹn pipeline PySpark GIGO và chuẩn bị dữ liệu mẫu cho bài toán tìm kiếm tương đồng.
3. **Vũ Văn Duy:** Đẩy nhanh tiến độ hoàn thiện module `benchmark_profiler.py` và dựng bản đồ kiểm chứng GIS Hàng Xanh `hang_xanh_gis_check.html`.
