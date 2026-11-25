import docx
from docx.shared import Pt, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn

template_path = r"D:\Trí tuệ nhân tạo\HK3\Trí tuệ nhân tạo cho hệ thống nhúng\3. Mẫu Báo cáo tuần x.docx"
out_path = r"d:\Trí tuệ nhân tạo\HK3\Phân tích dữ liệu lớn\Đồ án\Bao_Cao_Tuan_5_Phan_Tich_Du_Lieu_Lon.docx"

doc = docx.Document(template_path)

# 1. Update Title P0
p0 = doc.paragraphs[0]
p0.text = ""
p0.alignment = WD_ALIGN_PARAGRAPH.CENTER
r0 = p0.add_run("BÁO CÁO TUẦN 5 ĐỒ ÁN MÔN PHÂN TÍCH DỮ LIỆU LỚN (NHÓM 11)")
r0.font.name = "Times New Roman"
r0.font.size = Pt(13)
r0.bold = True

# 2. Update Project Name P1
p1 = doc.paragraphs[1]
p1.text = ""
r1_lbl = p1.add_run("Tên đồ án: ")
r1_lbl.font.name = "Times New Roman"
r1_lbl.font.size = Pt(13)
r1_lbl.bold = True

r1_val = p1.add_run(
    "“Xây dựng pipeline xử lý dữ liệu lớn mạng lưới giao thông trên cấu trúc đồ thị không–thời gian bằng Apache Spark và kỹ thuật tìm kiếm láng giềng gần đúng (LSH/ANN): Thực nghiệm đối chứng trên METR-LA/PEMS-BAY và ứng dụng phân tích tại TP.HCM kết hợp thời tiết”"
)
r1_val.font.name = "Times New Roman"
r1_val.font.size = Pt(13)
r1_val.italic = True

# 3. Update P2
p2 = doc.paragraphs[2]
p2.text = ""
r2 = p2.add_run("Thành viên thực hiện:")
r2.font.name = "Times New Roman"
r2.font.size = Pt(13)
r2.bold = True

# 4. Table 0: Student List without Phone and Email (3 Columns: STT, HỌ VÀ TÊN, MSSV)
table = doc.tables[0]
table.alignment = WD_TABLE_ALIGNMENT.CENTER

# Remove columns 4 (EMAIL) and 3 (ĐIỆN THOẠI) from the original template table
for row in table.rows:
    row._tr.remove(row.cells[4]._tc)
    row._tr.remove(row.cells[3]._tc)

tblGrid = table._tbl.tblGrid
if tblGrid is not None:
    cols = tblGrid.findall(qn("w:gridCol"))
    if len(cols) >= 5:
        tblGrid.remove(cols[4])
        tblGrid.remove(cols[3])

students = [
    ("1", "Trần Hoàng Hôn (Trưởng nhóm - STT 52)", "26410046"),
    ("2", "Nguyễn Duy Khang (STT 55)", "26410055"),
    ("3", "Phạm Thành Trung (STT 70)", "26410141"),
    ("4", "Vũ Văn Duy (STT 47)", "26410031"),
]

while len(table.rows) < 5:
    table.add_row()

col_widths = [Inches(0.8), Inches(4.2), Inches(1.5)]

# Header row styling
header_cells = table.rows[0].cells
for idx, text in enumerate(["STT", "HỌ VÀ TÊN", "MSSV"]):
    header_cells[idx].text = ""
    p = header_cells[idx].paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(text)
    r.font.name = "Times New Roman"
    r.font.size = Pt(12)
    r.bold = True

# Data rows styling
for r_idx, s in enumerate(students, 1):
    row = table.rows[r_idx]
    for c_idx, val in enumerate(s):
        cell = row.cells[c_idx]
        cell.text = ""
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER if c_idx in (0, 2) else WD_ALIGN_PARAGRAPH.LEFT
        r = p.add_run(val)
        r.font.name = "Times New Roman"
        r.font.size = Pt(12)
        if c_idx == 1 and "Trưởng nhóm" in val:
            r.bold = True

for row in table.rows:
    for idx, width in enumerate(col_widths):
        row.cells[idx].width = width

# Delete old paragraphs below table (from index 3 onwards)
p_elements = [p._p for p in doc.paragraphs[3:]]
for pe in p_elements:
    pe.getparent().remove(pe)

# Helper function to add a major numbered section
def add_section_header(doc, number_str, title_text):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(8)
    p.paragraph_format.space_after = Pt(3)
    p.paragraph_format.line_spacing = 1.15
    r_num = p.add_run(f"{number_str}. ")
    r_num.font.name = "Times New Roman"
    r_num.font.size = Pt(13)
    r_num.bold = True
    
    r_title = p.add_run(title_text)
    r_title.font.name = "Times New Roman"
    r_title.font.size = Pt(13)
    r_title.bold = True
    return p

# Helper function to add bullet points
def add_bullet_point(doc, bold_prefix, text_content, level=1):
    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Inches(0.25 * level)
    p.paragraph_format.first_line_indent = Inches(-0.15)
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    p.paragraph_format.line_spacing = 1.15
    
    r_bullet = p.add_run("• ")
    r_bullet.font.name = "Times New Roman"
    r_bullet.font.size = Pt(12)
    r_bullet.bold = True
    
    if bold_prefix:
        r_pre = p.add_run(bold_prefix + ": ")
        r_pre.font.name = "Times New Roman"
        r_pre.font.size = Pt(12)
        r_pre.bold = True
    
    r_text = p.add_run(text_content)
    r_text.font.name = "Times New Roman"
    r_text.font.size = Pt(12)
    return p

# --- 1. Nội dung công việc trong tuần ---
add_section_header(doc, "1", "Nội dung công việc trong tuần (Tuần 5: 18/11 – 24/11/2026)")
add_bullet_point(
    doc,
    "Tích hợp Toàn văn Mở đầu & Chương 1 vào Báo cáo Chính thức",
    "Hoàn thành soạn thảo và nạp toàn văn phần Mở đầu và Chương 1 (dung lượng 10–12 trang) vào file Word chính thức Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx; tích hợp Bảng đối chiếu 1.1 chi tiết giữa 5 công trình học thuật nền tảng IEEE/ACM và đề tài Nhóm 11.",
)
add_bullet_point(
    doc,
    "Soạn thảo Chuyên đề Lý thuyết Đồ thị Không gian G = (V, E, W)",
    "Xây dựng tài liệu lý luận chuyên sâu về đồ thị có hướng, ma trận khoảng cách mạng lưới Dijkstra, hàm nhân Gaussian ngưỡng cắt (kappa, epsilon) và phân tích ma trận trọng số thực địa trích xuất từ OpenStreetMap tại Ngã tư Hàng Xanh (20 nút, 37 cạnh, độ thưa 33.8%) phục vụ Chương 2.",
)
add_bullet_point(
    doc,
    "Thiết kế Thuật toán Tiền xử lý & Nội suy Chuỗi thời gian GIGO",
    "Xây dựng thuật toán xử lý dữ liệu rác GIGO trên PySpark: nội suy tuyến tính cục bộ kết hợp forward-fill có giới hạn theo từng cảm biến để giải quyết triệt để 575.302 ô mang giá trị 0.0 mất tín hiệu (8.11% của METR-LA) mà không làm phá vỡ lưới thời gian đều 5 phút; lọc ngoại lai IQR.",
)
add_bullet_point(
    doc,
    "Khảo sát & Kiểm thử Thuật toán LSH vs. FAISS HNSW",
    "Thiết lập kịch bản smoke-test đối chứng giữa BucketedRandomProjectionLSH trên Spark MLlib và FAISS IndexHNSWFlat trên tập 10.000 vector trượt mẫu, kiểm tra tính tương thích và đo đạc sơ bộ Recall@10 cùng độ trễ truy vấn.",
)
add_bullet_point(
    doc,
    "Thiết kế Khung Đo kiểm Tài nguyên & Bản đồ GIS Tương tác",
    "Hoàn thiện module benchmark_profiler.py đo lường khách quan Wall-clock Time và Peak RAM qua psutil phục vụ so sánh hiệu năng Pandas vs. PySpark; xây dựng script trực quan hóa bản đồ GIS Folium kiểm chứng 20 nút Hàng Xanh và 207 cảm biến METR-LA đạt 100% PASS kiểm định nghiêm ngặt; hoàn thành trước kịch bản mở rộng 1M–17M dòng trên PEMS-BAY (PR #6 đã merged).",
)
add_bullet_point(
    doc,
    "Quản trị Tiến độ Dự án & Điều phối Nhóm",
    "Tạo GitHub Issue #5 đôn đốc các thành viên hoàn thành và nộp Pull Request trước 23:59 ngày 25/11; đồng bộ tiến độ tuần vào Ke_Hoach_Thuc_Hien_Do_An_Phan_Tich_Du_Lieu_Lon.xlsx và xây dựng kế hoạch chuyển tiếp sang Tuần 6.",
)

# --- 2. Phân công thực hiện ---
add_section_header(doc, "2", "Phân công thực hiện")
add_bullet_point(
    doc,
    "Trần Hoàng Hôn (Trưởng nhóm — MSSV: 26410046)",
    "Chịu trách nhiệm [H6] Hoàn tất tài liệu khảo sát 5 công trình học thuật IEEE/ACM và Biên bản góp ý Thầy Tân 16/11; [H7] Soạn thảo toàn văn Mở đầu & Chương 1 vào Khung Báo cáo Word và viết chuyên đề lý thuyết đồ thị G=(V, E, W) Hàng Xanh; [H8] Mở GitHub Issue #5 điều phối nhóm, xuất bản Báo cáo Tuần 4 và Tuần 5, cập nhật tiến độ dự án.",
)
add_bullet_point(
    doc,
    "Nguyễn Duy Khang (MSSV: 26410055)",
    "Chịu trách nhiệm [K3] Xây dựng Vectorizer cửa sổ trượt và kịch bản smoke-test LSH vs. FAISS trên 10.000 vector mẫu; [K4] Tối ưu hóa phân tán PySpark, cấu hình bộ nhớ driver và chuẩn bị kịch bản lát cắt dữ liệu quy mô triệu dòng (1M – 17M).",
)
add_bullet_point(
    doc,
    "Phạm Thành Trung (MSSV: 26410141)",
    "Chịu trách nhiệm [T4] Xây dựng thuật toán tiền xử lý chuỗi thời gian nội suy ô 0.0 mất tín hiệu và lọc ngoại lai IQR; [T5] Thiết kế pipeline chuẩn hóa thang đo đa biến (Min-Max/Z-score) trên PySpark và đóng gói dữ liệu sạch Parquet phân vùng theo tháng.",
)
add_bullet_point(
    doc,
    "Vũ Văn Duy (MSSV: 26410031)",
    "Chịu trách nhiệm [D1] Hoàn thiện khung đo kiểm tài nguyên benchmark_profiler.py và smoke-test đối chứng Pandas vs. PySpark trên lát cắt dữ liệu mẫu; [D2] Dựng bản đồ tương tác GIS Folium kiểm chứng thực địa nút giao Hàng Xanh và mạng lưới cảm biến METR-LA; [D3] Triển khai sớm kịch bản scaling benchmark 1M–17M dòng kèm OOM watchdog (toàn bộ đã hoàn tất và hợp nhất tại PR #6).",
)

# --- 3. Báo cáo quá trình thực hiện ---
add_section_header(doc, "3", "Báo cáo quá trình thực hiện (Kết quả đạt được)")
add_bullet_point(
    doc,
    "Về Quyển Báo cáo Đồ án & Khung Lý thuyết",
    "Đã đưa toàn văn Phần Mở đầu, Chương 1 và Bảng đối chiếu 1.1 vào Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx (tổng cộng 213 đoạn văn, 35 tiêu đề chuẩn học thuật); hoàn thành tài liệu chuyên đề lý thuyết đồ thị không gian G=(V, E, W) và ma trận trọng số Gaussian Hàng Xanh tại 05_report_slides/draft_chuong_2_do_thi_g_v_e_w.md sẵn sàng tích hợp vào Chương 2.",
)
add_bullet_point(
    doc,
    "Về Tiền xử lý Dữ liệu Chuỗi thời gian GIGO",
    "Hoàn thiện thuật toán làm sạch phân tán trên PySpark: nội suy tuyến tính cục bộ kết hợp forward-fill có giới hạn theo từng cảm biến giúp triệt tiêu 575.302 ô giá trị 0.0 nhân tạo của METR-LA, đảm bảo tính liên tục của lưới thời gian 5 phút phục vụ trích xuất vector đặc trưng; xác lập quy trình chuẩn hóa thang đo Min-Max/Z-score.",
)
add_bullet_point(
    doc,
    "Về Thuật toán LSH/ANN & Khung Đo lường Hiệu năng",
    "Đã hoàn thành thiết kế khung kiểm thử hiệu năng benchmark_profiler.py (đo độ trễ mili-giây và bộ nhớ RAM đỉnh thực tế RSS bằng psutil); xây dựng xong module đối chứng thuật toán LSH và FAISS HNSW trên tập vector trượt mẫu; đặc biệt đã hoàn thành trước đợt thực nghiệm lớn 1M–17M dòng trên PEMS-BAY với 3 biểu đồ chuẩn ấn phẩm.",
)
add_bullet_point(
    doc,
    "Về Quản trị Dự án & Kiểm soát Tiến độ",
    "Xuất bản thành công Báo cáo Tuần 4 và Báo cáo Tuần 5 theo đúng biểu mẫu chuẩn của trường (đã chuẩn hóa bảng danh sách 3 cột, không thông tin cá nhân ngoài danh mục); mở GitHub Issue #5 kiểm soát chặt chẽ tiến độ; hoàn tất review và merge thành công PR #6; cập nhật tiến độ Tuần 4 đạt 100% và Tuần 5 đạt 90%.",
)

# --- 4. Khó khăn / Thách thức và Giải pháp ---
add_section_header(doc, "4", "Khó khăn / Thách thức và Giải pháp")
add_bullet_point(
    doc,
    "Thách thức về Thời gian & Kỳ thi Giữa kỳ",
    "Tuần 6 diễn ra kỳ thi giữa kỳ môn Phân tích Dữ liệu Lớn (hình thức tự luận viết tay 90 phút trên lớp ngày 25/11), đòi hỏi các thành viên phải cân đối quỹ thời gian giữa việc ôn tập lý thuyết học phần (HDFS, MapReduce, Spark DAG/RDD, LSH, KNN) và triển khai thực nghiệm đồ án.",
)
add_bullet_point(
    doc,
    "Giải pháp Điều phối Linh hoạt",
    "Trưởng nhóm đã chủ động hoàn thành trước toàn bộ phần lý thuyết Chương 1 và khung Chương 2; phân chia mốc nộp PR code trước 23:59 ngày 25/11 để các thành viên tập trung tối đa cho kỳ thi; tổ chức thảo luận trực tuyến ngắn để ôn tập chéo các chủ đề thi trọng tâm.",
)

# --- 5. Dự kiến công việc tuần tiếp theo ---
add_section_header(doc, "5", "Dự kiến công việc tuần tiếp theo (Tuần 6: 25/11 – 01/12/2026)")
add_bullet_point(
    doc,
    "Tham gia Kỳ thi Giữa kỳ",
    "Toàn bộ 4 thành viên tham gia kỳ thi giữa kỳ 90 phút viết tay trên lớp vào chiều/tối Thứ Hai ngày 25/11/2026.",
)
add_bullet_point(
    doc,
    "Nghiệm thu Mã nguồn & Hợp nhất Pull Request",
    "Review kỹ thuật, giải quyết các xung đột và merge toàn bộ PR của Trung ([T4], [T5]), Khang ([K3], [K4]), Duy ([D1], [D2]) lên nhánh main GitHub repo mrkiss-it/bigdata-traffic-graph-spark-lsh.",
)
add_bullet_point(
    doc,
    "Triển khai Thực nghiệm Benchmark Quy mô Triệu dòng",
    "Chạy kịch bản thực nghiệm chính thức: KB2 đối chứng hiệu năng Pandas vs. PySpark trên các lát cắt 1M, 5M, 10M và toàn bộ 16.94M dòng; KB3 đối chứng Exact KNN vs. LSH vs. FAISS HNSW đo đạc toàn diện Recall@10, Query Latency và Peak RAM.",
)
add_bullet_point(
    doc,
    "Chấp bút Soạn thảo Toàn văn Chương 2 & Mở khung Chương 3",
    "Viết toàn văn Chương 2 (dung lượng > 13 trang chuyên sâu cơ sở toán học và kiến trúc phân tán) và nạp số liệu thực nghiệm vào Chương 3 của Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx.",
)

doc.save(out_path)
print("SUCCESS: Generated Week 5 Report at:", out_path)
