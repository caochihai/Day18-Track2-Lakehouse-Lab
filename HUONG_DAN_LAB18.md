# 📘 Hướng dẫn thực hiện Lab 18 — Data Lakehouse Architecture

Lab này yêu cầu bạn xây dựng một pipeline dữ liệu Bronze → Silver → Gold sử dụng **Delta Lake**. Có hai lộ trình (Path): **Lightweight** (khuyên dùng) và **Spark**. Hướng dẫn dưới đây tập trung vào lộ trình Lightweight.

---

## 🚀 Bước 1: Thiết lập môi trường (Setup)

Trước khi bắt đầu, hãy đảm bảo bạn đã cài đặt Python ≥ 3.10.

1. **Khởi tạo môi trường và cài đặt thư viện:**
   ```bash
   make setup
   ```
2. **Kiểm tra nhanh (Smoke test):**
   ```bash
   make smoke
   ```
   *Nếu thông báo "All checks passed" hiện ra, bạn đã sẵn sàng.*

3. **Tạo dữ liệu mẫu (Cần thiết cho NB4):**
   ```bash
   make data
   ```

4. **Mở Jupyter Lab:**
   ```bash
   make lab
   ```

---

## 🛠️ Bước 2: Thực hiện các Notebook nhiệm vụ

Bạn cần hoàn thành và chạy hết 4 file trong thư mục `notebooks/`.

### 1. NB1 — Delta Lake Basics (`01_delta_basics.py`)
*   **Mục tiêu:** Hiểu cách ghi/đọc Delta table và cơ chế kiểm soát schema.
*   **Nhiệm vụ:**
    *   Ghi một DataFrame vào đường dẫn Delta.
    *   Kiểm tra thư mục `_delta_log/` để thấy các file JSON (Transaction Log).
    *   Thử ghi dữ liệu sai kiểu (ví dụ `age="thirty"`) và xác nhận nó bị chặn bởi **Schema Enforcement**.
    *   Dùng `schema_mode="merge"` để thêm cột mới (`tier`) vào table hiện có (**Schema Evolution**).

### 2. NB2 — Optimize & Z-Order (`02_optimize_zorder.py`)
*   **Mục tiêu:** Giải quyết vấn đề "Small-file problem" và tối ưu hóa truy vấn.
*   **Nhiệm vụ:**
    *   Chạy vòng lặp tạo ra 200 file nhỏ để thấy hiệu năng bị giảm.
    *   Chạy lệnh `compact()` để gộp các file nhỏ.
    *   Chạy lệnh `z_order(["user_id"])` để sắp xếp dữ liệu giúp bỏ qua file (file-skipping) khi truy vấn.
    *   **Yêu cầu:** Tốc độ truy vấn sau khi tối ưu phải nhanh hơn ≥ 3 lần HOẶC tỷ lệ file bị loại bỏ (pruned) ≥ 10 lần.

### 3. NB3 — Time Travel & Merge (`03_time_travel.py`)
*   **Mục tiêu:** Quản lý phiên bản dữ liệu và cập nhật dữ liệu thông minh.
*   **Nhiệm vụ:**
    *   Thực hiện lệnh `MERGE` để cập nhật (update) và chèn mới (insert) 100K dòng dữ liệu cùng lúc.
    *   Sử dụng `history()` để xem danh sách các phiên bản của table.
    *   Sử dụng tính năng **Time Travel** để truy vấn lại dữ liệu tại một phiên bản cũ.
    *   Sử dụng lệnh `restore()` để quay ngược trạng thái table về trước khi bị ghi đè dữ liệu lỗi.

### 4. NB4 — Medallion Architecture (`04_medallion.py`)
*   **Mục tiêu:** Xây dựng pipeline hoàn chỉnh Bronze → Silver → Gold cho dữ liệu LLM Observability.
*   **Nhiệm vụ:**
    *   **Bronze:** Kiểm tra dữ liệu thô đã load.
    *   **Silver:** Làm sạch dữ liệu (parse JSON, loại bỏ bản ghi lỗi, xóa trùng lặp theo `request_id`).
    *   **Gold:** Tổng hợp dữ liệu theo ngày và model (tính latency p50/p95, tổng token, chi phí USD).
    *   **Yêu cầu:** Bảng Gold phải có dữ liệu trải dài trên ≥ 7 ngày.

---

## 📸 Bước 3: Thu thập minh chứng (Deliverables)

Sau khi chạy xong, bạn cần chuẩn bị các mục sau để nộp bài:

1.  **4 Notebook đã chạy:** Lưu lại file `.ipynb` (hoặc `.py` nếu dùng Jupytext) kèm theo kết quả đầu ra ở các cell.
2.  **Ảnh chụp màn hình:**
    *   Cấu trúc thư mục `_lakehouse/` trên disk.
    *   Nội dung của một file JSON trong `_delta_log/`.
3.  **File phản hồi:** Tạo file `submission/REFLECTION.md` (không quá 200 từ) trả lời: *Trong các lỗi (anti-patterns) ở slide chương 5, đội của bạn dễ mắc phải lỗi nào nhất? Tại sao?*

---

## 🏆 Bước 4: Nộp bài (Submission)

1.  **Fork** repository này về tài khoản GitHub cá nhân.
2.  **Commit** các file notebook và ảnh chụp màn hình vào fork của bạn.
3.  **Mở Pull Request (PR)** về repo gốc với tiêu đề: `[Mã_Lớp] Lab18 — [Họ Tên]`.

---

*Lưu ý: Nếu bạn muốn thử thách thêm, hãy đọc file `BONUS-CHALLENGE.md` để thiết kế kiến trúc Lakehouse cho các bài toán quy mô lớn (không tính điểm nhưng được giảng viên review trực tiếp).*
