import docx
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn

template_path = r"D:\Trí tuệ nhân tạo\HK3\Trí tuệ nhân tạo cho hệ thống nhúng\3. Mẫu Báo cáo tuần x.docx"
out_path = r"d:\Trí tuệ nhân tạo\HK3\Phân tích dữ liệu lớn\Đồ án\Bao_Cao_Tuan_4_Phan_Tich_Du_Lieu_Lon.docx"
mau_path = r"d:\Trí tuệ nhân tạo\HK3\Phân tích dữ liệu lớn\Đồ án\Mau_Bao_Cao_Tuan_Phan_Tich_Du_Lieu_Lon.docx"

doc = docx.Document(template_path)

# 1. Update Title P0
p0 = doc.paragraphs[0]
p0.text = ""
p0.alignment = WD_ALIGN_PARAGRAPH.CENTER
r0 = p0.add_run("BÁO CÁO TUẦN 4 ĐỒ ÁN MÔN PHÂN TÍCH DỮ LIỆU LỚN (NHÓM 11)")
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

# 4. Table 0: Student List
table = doc.tables[0]
table.alignment = WD_TABLE_ALIGNMENT.CENTER

students = [
    ("1", "Trần Hoàng Hôn (Trưởng nhóm - STT 52)", "26410046", "0987 654 321", "mrkiss.it@gmail.com"),
    ("2", "Nguyễn Duy Khang (STT 55)", "26410055", "0912 345 678", "ndk1200@gmail.com"),
    ("3", "Phạm Thành Trung (STT 70)", "26410141", "0923 456 789", "ttrung.cs@gmail.com"),
    ("4", "Vũ Văn Duy (STT 47)", "26410031", "0934 567 890", "duyvu.citd@gmail.com"),
]

# Ensure table has 5 rows (Header + 4 students)
while len(table.rows) < 5:
    table.add_row()

col_widths = [Inches(0.5), Inches(2.5), Inches(1.0), Inches(1.3), Inches(2.1)]

# Header row styling
header_cells = table.rows[0].cells
for idx, text in enumerate(["STT", "HỌ VÀ TÊN", "MSSV", "ĐIỆN THOẠI", "EMAIL"]):
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
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER if c_idx in (0, 2, 3) else WD_ALIGN_PARAGRAPH.LEFT
        r = p.add_run(val)
        r.font.name = "Times New Roman"
        r.font.size = Pt(12)
        if c_idx == 1 and "Trưởng nhóm" in val:
            r.bold = True

for row in table.rows:
    for idx, width in enumerate(col_widths):
        row.cells[idx].width = width

# Delete old paragraphs below table (from index 3 onwards)
# Note: In python-docx, deleting paragraphs requires removing them from the XML tree
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
add_section_header(doc, "1", "Nội dung công việc trong tuần (Tuần 4: 11/11 – 17/11/2026)")
add_bullet_point(
    doc,
    "Hoàn thiện Đề cương & Đăng ký đề tài",
    "Rà soát, chốt Đề cương chi tiết 7 mục theo định dạng bảng chuẩn CITD/UIT; xác nhận trạng thái đăng ký đề tài tại Hàng 33 trên file theo dõi của lớp (Final_Project.xlsx) trình TS. Hà Minh Tân.",
)
add_bullet_point(
    doc,
    "Khởi tạo Kiến trúc Mã nguồn",
    "Thiết lập cấu trúc kho lưu trữ GitHub gồm 5 phân hệ chuẩn hóa (01_data_ingestion/, 02_pyspark_gigo/, 03_benchmarks_similarity/, 04_hcm_dashboard/, 05_report_slides/), cấu hình .gitignore bỏ qua dữ liệu nặng và hoàn thiện requirements.txt.",
)
add_bullet_point(
    doc,
    "Nạp & Kiểm toán Dữ liệu Giao thông Lớn (GIGO)",
    "Tải tự động và kiểm tra mã băm MD5 hai bộ dữ liệu benchmark quốc tế: METR-LA (7.09M dòng) và PEMS-BAY (16.94M dòng); thực hiện kiểm toán toàn diện lỗi 0.0 (sentinel mất kết nối), NaN, ngoại lai vật lý (> 80 mph) và phân vị IQR.",
)
add_bullet_point(
    doc,
    "Trích xuất Đồ thị Không gian TP.HCM & API Thời tiết",
    "Trích xuất mạng lưới đường bộ cụm Hàng Xanh (25 nút trọng điểm trên 6 trục đường chính) từ OpenStreetMap bằng OSMnx, tính toán ma trận kề trọng số Gaussian W_ij; thu thập mẫu thời tiết 90 ngày từ API Open-Meteo.",
)
add_bullet_point(
    doc,
    "Kiểm thử Hạ tầng PySpark & Data Contract Parquet",
    "Kiểm thử môi trường Apache Spark 3.5.9, PyArrow và thư viện FAISS (vượt qua 6/6 bài test); chốt Data Contract Parquet 6 cột nén Snappy có phân vùng theo tháng (year_month) giúp giảm dung lượng 7.1x.",
)
add_bullet_point(
    doc,
    "Khảo sát Học thuật & Làm việc cùng Giảng viên",
    "Tổng hợp danh mục 5 công trình học thuật nền tảng chuẩn IEEE/ACM và xác lập 3 khoảng trống nghiên cứu (Research Gap); làm việc trực tiếp với TS. Hà Minh Tân tối 16/11 để chốt phương pháp luận.",
)

# --- 2. Phân công thực hiện ---
add_section_header(doc, "2", "Phân công thực hiện")
add_bullet_point(
    doc,
    "Trần Hoàng Hôn (Trưởng nhóm — MSSV: 26410046)",
    "Chịu trách nhiệm [H1] Chốt Đề cương chi tiết và đăng ký Hàng 33; [H2] Khởi tạo cấu trúc Repo 5 thư mục và requirements.txt; [H3] Viết script extract_hang_xanh_graph.py trích xuất 25 nút đồ thị Hàng Xanh và ma trận Gaussian W_ij; [H4] Khảo sát 5 công trình IEEE/ACM và Research Gap (literature_survey_notes.md); [H5] Trao đổi Thầy Tân tối 16/11, lập biên bản góp ý và điều phối tiến độ toàn nhóm.",
)
add_bullet_point(
    doc,
    "Nguyễn Duy Khang (MSSV: 26410055)",
    "Chịu trách nhiệm [K1] Thiết lập và kiểm thử môi trường PySpark/FAISS, hoàn thành script spark_env_check.py (đạt 6/6 PASS); [K2] Thiết kế Data Contract Parquet 6 cột (parquet_schema_contract.md) và xây dựng script PySpark csv_to_partitioned_parquet.py phân vùng theo tháng.",
)
add_bullet_point(
    doc,
    "Phạm Thành Trung (MSSV: 26410141)",
    "Chịu trách nhiệm [T1] Viết script download_traffic_datasets.py tải dữ liệu CSV METR-LA & PEMS-BAY và convert_traffic_to_parquet.py chuyển đổi nhanh; [T2] Viết connector fetch_open_meteo.py thu thập 90 ngày thời tiết Hàng Xanh; [T3] Chạy kiểm toán dữ liệu rác GIGO, xuất báo cáo gigo_audit_report.md và các bảng thống kê theo cảm biến/giờ.",
)
add_bullet_point(
    doc,
    "Vũ Văn Duy (MSSV: 26410031)",
    "Chịu trách nhiệm [D1] Thiết kế kiến trúc bộ đo kiểm tài nguyên benchmark_profiler.py (đo Wall-clock Time và Peak RAM qua psutil); [D2] Thiết kế bản đồ trực quan GIS Folium kiểm chứng thực địa nút giao Hàng Xanh và cụm cảm biến METR-LA.",
)

# --- 3. Báo cáo quá trình thực hiện ---
add_section_header(doc, "3", "Báo cáo quá trình thực hiện (Kết quả đạt được)")
add_bullet_point(
    doc,
    "Về Hồ sơ & Đề cương môn học",
    "Đề cương chi tiết De_Cuong_Chi_Tiet_Do_An_Phan_Tich_Du_Lieu_Lon.docx đã được TS. Hà Minh Tân thông qua và ký duyệt; nhóm đã xác nhận đăng ký chính thức đề tài tại Hàng 33 file Final_Project.xlsx.",
)
add_bullet_point(
    doc,
    "Về Thu thập dữ liệu & Kiểm toán chất lượng GIGO",
    "Tải thành công 100% dữ liệu: METR-LA (34.272 mốc thời gian x 207 cảm biến = 7.094.304 ô) và PEMS-BAY (52.116 mốc x 325 cảm biến = 16.937.700 ô). Báo cáo kiểm toán gigo_audit_report.md phát hiện METR-LA có 575.302 ô mang giá trị 0.0 (chiếm 8.11%), xác định là lỗi mất tín hiệu của trạm đo cần xử lý nội suy; 0 ô NaN; 0 ngoại lai vật lý (<0 hoặc >80 mph). Kéo thành công 2.160 giờ dữ liệu thời tiết Hàng Xanh.",
)
add_bullet_point(
    doc,
    "Về Trích xuất Đồ thị Không gian TP.HCM",
    "Xuất hoàn chỉnh đồ thị 25 nút trọng điểm và các cung đường trên 6 trục chính cụm Hàng Xanh, tính toán ma trận trọng số Gaussian W_ij lưu tại hang_xanh_adjacency_gaussian.csv và hang_xanh.graphml.",
)
add_bullet_point(
    doc,
    "Về Hạ tầng Phân tán & Lưu trữ Cột Parquet",
    "Script spark_env_check.py vượt qua 6/6 bài kiểm tra (Spark 3.5.9, Arrow không fallback, Parquet snappy, FAISS IndexFlatL2). Script csv_to_partitioned_parquet.py chuyển đổi phân vùng thành công: METR-LA nén từ 69.4 MB CSV xuống 9.8 MB Parquet (nhỏ hơn 7.1 lần); PEMS-BAY nén từ 81.7 MB xuống 25.4 MB Parquet (nhỏ hơn 3.2 lần).",
)
add_bullet_point(
    doc,
    "Về Khảo sát Học thuật & Quản lý Mã nguồn",
    "Hoàn thành tài liệu tổng hợp 5 công trình học thuật IEEE/ACM (literature_survey_notes.md), biên bản góp ý Thầy Tân (bien_ban_gop_y_thay_tan_16_11.md); hợp nhất 4 Pull Request lên nhánh main GitHub repo mrkiss-it/bigdata-traffic-graph-spark-lsh. Tiến độ Tuần 4 hoàn thành 100% mục tiêu đề ra.",
)

# --- 4. Dự kiến công việc tuần tiếp theo ---
add_section_header(doc, "4", "Dự kiến công việc tuần tiếp theo (Tuần 5: 18/11 – 24/11/2026)")
add_bullet_point(
    doc,
    "Xây dựng Pipeline PySpark GIGO",
    "Triển khai module tiền xử lý dữ liệu phân tán trên PySpark: nội suy giá trị mất tín hiệu 0.0, xử lý ngoại lai IQR và chuẩn hóa Min-Max/Z-score trên các phân vùng Parquet.",
)
add_bullet_point(
    doc,
    "Soạn thảo Báo cáo Đồ án (Mở đầu & Chương 1)",
    "Chấp bút viết toàn văn phần Mở đầu và Chương 1 (Tổng quan, Phân tích 5V Big Data, Khảo sát công trình liên quan & Research Gap) đạt dung lượng 10–12 trang vào Khung_Bao_Cao_Do_An_Phan_Tich_Du_Lieu_Lon.docx.",
)
add_bullet_point(
    doc,
    "Thực hiện Smoke-test Đo tài nguyên & Thuật toán LSH/ANN",
    "Chạy thử nghiệm module benchmark_profiler.py (so sánh Pandas vs. PySpark trên 100K–1M dòng) và smoke-test thuật toán BucketedRandomProjectionLSH cùng FAISS HNSW trên 10.000 vector trượt mẫu.",
)
add_bullet_point(
    doc,
    "Dựng Bản đồ Tương tác GIS Folium",
    "Hoàn thiện script build_gis_inspection_map.py xuất 2 file bản đồ HTML (hang_xanh_gis_check.html và metr_la_sensors_map.html) phục vụ kiểm chứng thực địa và tích hợp Dashboard.",
)

# Save to both target file and template in Do An
doc.save(out_path)
doc.save(mau_path)
print("SUCCESS: Generated both files successfully!")
