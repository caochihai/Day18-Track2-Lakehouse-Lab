# 🌟 Hướng dẫn thử thách Bonus — Thiết kế Kiến trúc Lakehouse

Thử thách này không bắt buộc và không tính điểm vào bài Lab chính, nhưng là cơ hội để bạn nhận phản hồi chi tiết từ giảng viên về tư duy thiết kế hệ thống (Architecture Thinking).

---

## 📂 1. Sản phẩm cần nộp
1. **Tài liệu chính:** `submission/bonus/ARCHITECTURE.md` (Dài 3–6 trang Markdown).
2. **Code minh chứng (PoC - Tùy chọn):** `submission/bonus/poc/` (Một notebook demo phần khó nhất trong thiết kế).

---

## 📝 2. Cấu trúc tài liệu `ARCHITECTURE.md`
Tài liệu của bạn phải bao gồm đầy đủ các mục sau:

### I. Problem Statement (≤ 200 từ)
- Mô tả bài toán bạn chọn.
- Các con số thực tế: Quy mô dữ liệu (TB/ngày), độ trễ yêu cầu (Latency), ngân sách (Budget).
- Tại sao bài toán này lại khó?

### II. Architecture Diagram
- Sơ đồ luồng dữ liệu từ Nguồn → Bronze → Silver → Gold.
- Các thành phần công nghệ sử dụng (Ví dụ: Kafka, Delta Lake, Trino, S3...).
- *Lưu ý:* Có thể dùng ASCII art hoặc chèn ảnh từ các công cụ như Mermaid, Draw.io.

### III. Quyết định Kiến trúc & Lựa chọn thay thế (Quan trọng nhất)
Đưa ra ít nhất **5 quyết định lớn**. Với mỗi quyết định, hãy viết theo cấu trúc:
- **Lựa chọn:** Tôi chọn công nghệ/phương pháp **A**.
- **Lý do:** Tại sao chọn A? (Ưu điểm về chi phí, tốc độ, hoặc tính năng).
- **Lựa chọn bị loại:** Tôi đã xem xét **B** và **C** nhưng loại bỏ vì [Trade-off cụ thể].

### IV. Kịch bản lỗi (Failure Modes)
Đưa ra ít nhất **3 tình huống** hệ thống bị lỗi (ví dụ: dữ liệu đến muộn, sai schema, sập server lúc 3h sáng):
- Cách phát hiện lỗi.
- Quy trình khắc phục (Rollback) sử dụng các tính năng như Time Travel.

### V. Ước lượng chi phí (Cost Estimation)
- Tính toán chi phí dự kiến hàng tháng (Storage + Compute).
- Ví dụ: `$0.023/GB * 100TB + $2/giờ * 24h * 30 ngày`.

### VI. Kế hoạch triển khai MVP (1 tuần)
- Bạn sẽ làm gì trong tuần đầu tiên để chứng minh kiến trúc này khả thi?

---

## 💡 3. Các chủ đề gợi ý (Chọn 1)
- **Topic A:** LLM Observability (1 tỷ request/ngày).
- **Topic B:** Trillion-token Training Corpus (Quản lý dữ liệu AI khổng lồ).
- **Topic C:** CDC cho Ride-hailing Việt Nam (Tuân thủ Nghị định 13).
- **Topic D:** Multimodal RAG (10 triệu văn bản pháp luật).
- **Topic E:** FinOps cho Click-stream (Ngân sách cố định $8K/tháng).
- **Topic F:** Migration Catalog zero-downtime.
- **Topic G:** Real-time Feature Store cho Ngân hàng.

---

## ✅ 4. Danh sách kiểm tra (Self-Checklist)
Trước khi nộp, hãy tự hỏi:
- [ ] Mình có đưa ra ít nhất 5 quyết định kèm theo các phương án bị loại không?
- [ ] Các con số (TB, ms, $) có thực tế không?
- [ ] Mình có áp dụng các khái niệm Day 18 (Medallion, ACID, Time Travel, Lineage...) không?
- [ ] Kịch bản lỗi có phương án xử lý cụ thể không?

---

## 🚀 5. Cách nộp bài
Push tài liệu vào thư mục `submission/bonus/` trong fork của bạn và gửi Pull Request chung với bài Lab chính. Tiêu đề PR thêm hậu tố `[+bonus]`.
