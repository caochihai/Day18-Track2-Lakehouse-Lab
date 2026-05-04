# Reflection: Lakehouse Anti-patterns

- **Họ và tên:** Cao Chí Hải
- **Mã học viên:** 2A202600011
- **Lớp:** E403

---

Trong số các anti-pattern ở slide §5, **"Small-file problem" (Vấn đề quá nhiều file nhỏ)** là rủi ro lớn nhất mà hệ thống dữ liệu của team tôi dễ vướng phải nhất.

**Lý do:**
Trong thực tế, hệ thống thường tiếp nhận dữ liệu liên tục qua các streaming pipelines (như Kafka, CDC) với tần suất cao. Nếu cứ mỗi micro-batch đổ về Bronze layer lại tạo ra một file Parquet mới mà không có cơ chế quản lý, Data Lake sẽ nhanh chóng bị băm nát thành hàng triệu file siêu nhỏ (chỉ vài KB). 

Hậu quả là khi truy vấn, engine phân tích (như Spark, DuckDB hay Trino) sẽ tốn phần lớn thời gian (overhead) chỉ để quét metadata, mở và đóng file thay vì thực sự đọc dữ liệu. Điều này làm hiệu năng hệ thống sụt giảm nghiêm trọng (như benchmark trước khi tối ưu trong NB2).

**Giải pháp:**
Để phòng tránh, team cần thiết lập các job chạy ngầm định kỳ (ví dụ: mỗi đêm) để thực thi lệnh `OPTIMIZE` (gộp file nhỏ thành file lớn có kích thước chuẩn ~256MB/1GB) kết hợp với `Z-ORDER` theo các cột thường xuyên được `filter`. Điều này không chỉ dọn dẹp hệ thống mà còn tối ưu hóa tính năng Data Skipping.
