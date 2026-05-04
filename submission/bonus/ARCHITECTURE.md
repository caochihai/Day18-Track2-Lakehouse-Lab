# Bonus Challenge: CDC từ Ride-Hailing Việt Nam → Lakehouse (Tuân thủ Nghị định 13)

- **Họ và tên:** Cao Chí Hải
- **Mã học viên:** 2A202600011
- **Lớp:** E403
- **Topic:** C — Vietnamese ride-hailing CDC → Lakehouse (Decree 13 compliant)

---

## I. Problem Statement

Một nền tảng ride-hailing tại Việt Nam (tương tự Grab/Be/Xanh SM) vận hành hệ thống production Oracle Database để lưu trữ dữ liệu chuyến đi. Quy mô:

- **100 triệu chuyến/năm**, tương đương ~274K chuyến/ngày.
- **30,000 writes/giây** vào giờ cao điểm (7–9h sáng, 17–19h chiều).
- Mỗi bản ghi chứa **PII nhạy cảm**: số điện thoại tài xế/hành khách, số CMND/CCCD, tọa độ GPS chi tiết.

**Yêu cầu nghiệp vụ:**
- Dashboard phân tích phải refresh **trong vòng 60 giây** kể từ khi dữ liệu được commit tại Oracle.
- Truy vấn ad-hoc phải đạt **p95 < 1 giây**.
- Toàn bộ dữ liệu PII phải tuân thủ **Nghị định 13/2023/NĐ-CP** về bảo vệ dữ liệu cá nhân.
- Sự kiện đến muộn (late-arriving events) xảy ra thường xuyên do mất mạng ở các tỉnh xa.

**Tại sao khó?** Bài toán đồng thời yêu cầu (1) tốc độ gần real-time, (2) bảo mật PII ở mọi tầng, (3) xử lý dữ liệu đến muộn mà không làm sai lệch kết quả phân tích, và (4) duy trì audit trail đầy đủ cho mọi lần truy cập PII — tất cả trong khi giữ chi phí storage ở mức hợp lý.

---

## II. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PRODUCTION LAYER                                     │
│  ┌──────────┐     ┌───────────┐     ┌──────────────────┐                   │
│  │  Oracle   │────▶│ Debezium  │────▶│  Kafka (MSK)     │                   │
│  │  DB       │ CDC │ Connector │     │  topic: trips.*  │                   │
│  └──────────┘     └───────────┘     └────────┬─────────┘                   │
└──────────────────────────────────────────────┼─────────────────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LAKEHOUSE LAYER (S3 + Delta Lake)                   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  🥉 BRONZE — Raw CDC Events                                    │        │
│  │  ┌─────────────┐    ┌──────────────────────────────────┐       │        │
│  │  │ Kafka → S3   │───▶│ trips_raw (Delta)                │       │        │
│  │  │ (Spark SS)   │    │ • partition: ingestion_date      │       │        │
│  │  └─────────────┘    │ • PII tokenized at landing       │       │        │
│  │                      │ • raw JSON preserved             │       │        │
│  │                      └──────────────────────────────────┘       │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                               │                                             │
│                               ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  🥈 SILVER — Cleaned, Typed, SCD Type 2                        │        │
│  │  ┌──────────────────────────────────────────────────────┐      │        │
│  │  │ trips_cleaned (Delta)                                 │      │        │
│  │  │ • MERGE with late-data handling                       │      │        │
│  │  │ • SCD Type 2 for driver/rider status changes          │      │        │
│  │  │ • partition: event_date                               │      │        │
│  │  │ • Z-ORDER BY: city_id                                 │      │        │
│  │  └──────────────────────────────────────────────────────┘      │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                               │                                             │
│                               ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  🥇 GOLD — Business Metrics                                    │        │
│  │  ┌────────────────────┐  ┌────────────────────────────┐        │        │
│  │  │ daily_city_metrics │  │ driver_performance_weekly  │        │        │
│  │  │ • trips/city/day   │  │ • rating, cancel_rate      │        │        │
│  │  │ • avg fare, p95    │  │ • revenue per driver       │        │        │
│  │  │   wait time        │  │ • NO PII columns           │        │        │
│  │  └────────────────────┘  └────────────────────────────┘        │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │  📋 AUDIT TABLE — pii_access_log (Delta)                       │        │
│  │  • who, when, which_columns, purpose, approved_by              │        │
│  └─────────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                       │
│  ┌──────────┐     ┌──────────┐     ┌──────────────┐                        │
│  │  Trino   │     │ Superset │     │  Spark Jobs  │                        │
│  │ (ad-hoc) │     │(dashboard│     │ (batch ETL)  │                        │
│  └──────────┘     └──────────┘     └──────────────┘                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## III. Quyết định Kiến trúc Chính (5 Decisions)

### Quyết định 1: Table Format — Delta Lake

**Tôi chọn: Delta Lake.**

- **Loại bỏ Apache Iceberg** vì: Dù Iceberg có hỗ trợ multi-engine tốt (Trino, Spark, Flink), nhưng tính năng `MERGE INTO` — rất cần thiết cho bài toán CDC/SCD Type 2 — hoạt động ổn định và hiệu suất cao hơn ở Delta Lake nhờ Delete Vectors. Iceberg merge tại thời điểm hiện tại yêu cầu copy-on-write, tốn chi phí gấp ~2× cho bảng thường xuyên upsert.
- **Loại bỏ Apache Hudi** vì: Hudi được thiết kế cho CDC use-case (MOR mode), nhưng hệ sinh thái tooling tại Việt Nam (chủ yếu Databricks/EMR) hỗ trợ Delta tốt hơn nhiều. Hudi cũng có learning curve cao hơn và community nhỏ hơn, gây rủi ro khi tuyển dụng engineer mới.

### Quyết định 2: CDC Pipeline — Debezium → Kafka → Spark Structured Streaming

**Tôi chọn: Debezium + Kafka + Spark Structured Streaming.**

- **Loại bỏ Oracle GoldenGate** vì: GoldenGate là giải pháp CDC mạnh mẽ nhưng license cost cực cao (~$17,500/CPU). Debezium là open-source, hỗ trợ Oracle LogMiner connector, và output chuẩn Kafka — cho phép replay messages khi có lỗi. Trade-off: Debezium + LogMiner có latency cao hơn GoldenGate (~2-5 giây so với sub-second), nhưng hoàn toàn chấp nhận được với SLA 60 giây.
- **Loại bỏ AWS DMS** vì: DMS đơn giản hơn Debezium nhưng bị vendor lock-in vào AWS, không output ra Kafka native (phải qua Kinesis), và khó customize logic tokenization tại tầng ingestion.

### Quyết định 3: PII Protection — Tokenization tại Bronze Landing

**Tôi chọn: Tokenization (thay thế PII bằng token ngẫu nhiên) ngay khi dữ liệu chạm Bronze layer.** Mapping token ↔ giá trị gốc lưu trong một vault riêng biệt (HashiCorp Vault hoặc AWS Secrets Manager).

- **Loại bỏ Encryption-at-rest** vì: Mã hóa toàn bộ dữ liệu giải quyết vấn đề "data at rest" nhưng không giải quyết vấn đề "analyst vô tình thấy số điện thoại khi chạy `SELECT *`". Nghị định 13 yêu cầu kiểm soát truy cập ở mức column-level, encryption-at-rest không đáp ứng được.
- **Loại bỏ Dynamic Data Masking (DDM)** vì: DDM che dữ liệu tại thời điểm query, nhưng dữ liệu gốc vẫn nằm trên disk dưới dạng plaintext. Nếu ai đó truy cập trực tiếp file Parquet trên S3 (bypass query engine), PII vẫn bị lộ. Tokenization đảm bảo dữ liệu gốc **không bao giờ** tồn tại trên data lake.

### Quyết định 4: Late-Data Handling — MERGE với Timestamp Guard

**Tôi chọn: MERGE INTO với điều kiện `src.event_ts > tgt.event_ts`.**

```sql
MERGE INTO silver.trips_cleaned AS tgt
USING bronze_batch AS src
ON tgt.trip_id = src.trip_id
WHEN MATCHED AND src.event_ts > tgt.event_ts
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
```

- **Loại bỏ "Append-only + deduplicate later"** vì: Phương pháp này đơn giản (chỉ cần append) nhưng gây ra data inflation lớn — mỗi lần chạy dedup phải scan toàn bộ Silver table. Với 274K chuyến/ngày và late events có thể đến muộn 2-3 ngày, bảng sẽ phình nhanh và query performance suy giảm. MERGE xử lý idempotent tại chỗ.
- **Loại bỏ Watermark-based drop** vì: Spark watermark sẽ drop events đến quá muộn, nghĩa là mất dữ liệu. Trong ngành ride-hailing, một chuyến đi bị mất có thể gây sai lệch kết toán thu nhập tài xế — rủi ro pháp lý cao.

### Quyết định 5: Partitioning Strategy — Partition by `event_date`, Z-ORDER by `city_id`

**Tôi chọn: Partition theo ngày (`event_date`) + Z-ORDER theo thành phố (`city_id`).**

- **Loại bỏ Partition by `city_id`** vì: Việt Nam có 63 tỉnh thành nhưng phân bố chuyến đi rất lệch (TP.HCM và Hà Nội chiếm ~70%). Partition theo city sẽ tạo ra các partition cực kỳ chênh lệch kích thước (skewed partitions). Partition theo ngày cho kích thước partition đều hơn (~1-2GB/ngày) và phù hợp với pattern query phổ biến nhất: "phân tích dữ liệu tuần/tháng gần nhất".
- **Loại bỏ Partition by `hour`** vì: 24 partition/ngày × 365 ngày = 8,760 partition/năm. Quá nhiều partition nhỏ sẽ gây small-file problem và làm chậm metadata operations. Partition theo ngày + Z-ORDER theo city cho phép Delta pruning files hiệu quả mà không tạo quá nhiều partition.

---

## IV. Failure Modes (Kịch bản lỗi lúc 3 giờ sáng)

### Failure 1: Debezium Connector bị crash — mất CDC events

**Tình huống:** Debezium connector tới Oracle bị crash do OOM hoặc network timeout. Trong 30 phút downtime, khoảng ~54,000 sự kiện CDC bị miss (30K writes/s × 30 phút ÷ 1000 = 54K events ở peak, thực tế ít hơn vào ban đêm).

**Phát hiện:** 
- Alert khi Kafka consumer lag vượt ngưỡng 10,000 messages (PagerDuty).
- Alert khi Bronze table không có commit mới trong 5 phút (Delta `history()` monitor).

**Khắc phục:**
- Restart Debezium connector — nó tự resume từ Oracle LogMiner SCN (System Change Number) cuối cùng đã commit.
- Nếu SCN đã bị Oracle REDO log rotation xóa: chạy snapshot mode để re-sync toàn bộ bảng, sau đó dùng **Delta MERGE** để deduplicate tại Silver layer (idempotent by design).

### Failure 2: Schema thay đổi bất ngờ từ Oracle — Silver pipeline bị lỗi

**Tình huống:** Team backend thêm cột `surge_multiplier` (FLOAT) vào bảng Oracle `trips` mà không thông báo cho team data. Debezium bắt schema change và gửi message với field mới. Spark Structured Streaming job bị fail do schema mismatch tại Bronze → Silver MERGE.

**Phát hiện:**
- Spark job alert khi `AnalysisException: cannot resolve column` xuất hiện.
- Data contract validation check chạy mỗi 5 phút so sánh Bronze schema hiện tại với schema đã đăng ký.

**Khắc phục:**
- **Bước 1:** Dùng `schema_mode="merge"` tại Bronze layer để chấp nhận cột mới (Delta Schema Evolution) — Bronze không bao giờ reject data.
- **Bước 2:** Cập nhật Silver MERGE query để xử lý cột mới (thêm vào SELECT hoặc set default value).
- **Bước 3:** Nếu cột mới gây lỗi downstream, dùng **Delta Time Travel** để `RESTORE` Silver table về version trước khi schema change: `dt.restore(version_before_change)`. Điều này rollback toàn bộ dữ liệu sai trong vài giây.

### Failure 3: PII token mapping bị desync — dữ liệu không thể de-tokenize

**Tình huống:** HashiCorp Vault bị restart và mất cache. Khi team ops cần de-tokenize số điện thoại tài xế để xử lý khiếu nại khách hàng, vault trả về token mới thay vì mapping cũ → không thể phục hồi PII gốc.

**Phát hiện:**
- Health check endpoint của Vault trả về lỗi.
- De-tokenization service trả về `null` cho token đã biết → alert.

**Khắc phục:**
- Vault sử dụng Raft consensus với 3 replicas — tự heal khi 1 node down.
- Token mapping được backup hàng ngày vào S3 encrypted bucket riêng (AES-256, KMS managed).
- Worst case: restore từ backup + replay Bronze events (vì Bronze giữ raw CDC events bao gồm cả PII đã tokenized, mapping table cho phép reverse lookup).

---

## V. Ước lượng Chi phí (Back-of-Envelope)

### Storage (S3)

| Tầng | Dữ liệu/ngày | Retention | Tổng | Chi phí S3 Standard |
|------|---------------|-----------|------|----------------------|
| Bronze (raw CDC) | ~2 GB (compressed Parquet) | 90 ngày | 180 GB | $0.023/GB × 180 = **$4.14/tháng** |
| Silver (cleaned) | ~1.5 GB | 365 ngày | 548 GB | $0.023/GB × 548 = **$12.60/tháng** |
| Gold (aggregates) | ~50 MB | 730 ngày | 36 GB | $0.023/GB × 36 = **$0.83/tháng** |
| Audit log | ~100 MB | 1095 ngày (3 năm) | 110 GB | $0.023/GB × 110 = **$2.53/tháng** |

**Tổng Storage: ~$20/tháng**

> *Ghi chú tính toán:* 274K chuyến/ngày × ~1 KB/chuyến (sau nén Parquet với Snappy) = ~274 MB raw. Debezium CDC overhead (before/after image) ~7× → ~2 GB Bronze/ngày. Silver loại bỏ CDC metadata → ~1.5 GB. Gold chỉ là aggregation → ~50 MB.

### Compute

| Thành phần | Cấu hình | Giờ chạy/ngày | Chi phí |
|------------|----------|---------------|---------|
| Spark Structured Streaming (Bronze ingestion) | 2× m5.xlarge (4 vCPU, 16 GB) | 24h (always-on) | $0.192/h × 2 × 24 × 30 = **$276/tháng** |
| Spark Batch (Silver MERGE + Gold agg) | 2× m5.xlarge | 4h/ngày | $0.192/h × 2 × 4 × 30 = **$46/tháng** |
| OPTIMIZE + Z-ORDER job (nightly) | 1× m5.xlarge | 1h/ngày | $0.192/h × 1 × 30 = **$5.76/tháng** |
| Kafka (MSK) | kafka.m5.large × 3 brokers | 24h | ~**$250/tháng** |
| Trino (ad-hoc query) | 3× r5.xlarge | 12h/ngày (business hours) | $0.252/h × 3 × 12 × 30 = **$272/tháng** |

**Tổng Compute: ~$850/tháng**

### Tổng cộng

| Hạng mục | Chi phí/tháng |
|----------|---------------|
| Storage | $20 |
| Compute | $850 |
| HashiCorp Vault (managed) | $100 |
| Monitoring (Datadog/CloudWatch) | $50 |
| **Tổng** | **~$1,020/tháng** |

> Tổng chi phí ~$1,020/tháng cho hệ thống xử lý 100 triệu chuyến/năm là hợp lý (~$0.00012/chuyến). So với giải pháp dùng Snowflake hoặc Databricks managed (ước tính $3,000–5,000/tháng cho workload tương đương), giải pháp open-source tiết kiệm ~70%.

---

## VI. Kế hoạch MVP — Tuần đầu tiên

**Mục tiêu:** Chứng minh pipeline CDC → Bronze → Silver hoạt động end-to-end với tokenization, xử lý được late-arriving events.

### Ngày 1–2: Setup Infrastructure
- Deploy Kafka (single-broker, local Docker) + Debezium connector tới một PostgreSQL giả lập (thay Oracle).
- Tạo S3 bucket (hoặc MinIO local) cho Delta tables.

### Ngày 3–4: Bronze + Tokenization
- Viết Spark Structured Streaming job đọc từ Kafka → tokenize PII columns → ghi Delta Bronze.
- Test: Insert 10,000 rows giả lập vào PostgreSQL, verify Bronze table có đầy đủ dữ liệu với PII đã được tokenize.

### Ngày 5–6: Silver + Late-Data Handling
- Viết MERGE job từ Bronze → Silver với timestamp guard (`src.event_ts > tgt.event_ts`).
- Test: Insert dữ liệu với timestamp cũ (mô phỏng late-arriving), verify MERGE không ghi đè dữ liệu mới hơn.
- Chạy `OPTIMIZE + Z-ORDER BY (city_id)` trên Silver, benchmark query trước/sau.

### Ngày 7: Validation + Documentation
- Chạy `history()` trên Silver table — verify ≥ 5 versions.
- Test `RESTORE` — inject bad data, rollback, verify dữ liệu sạch.
- Viết README cho PoC với hướng dẫn chạy từ clean checkout.

**Thước đo thành công MVP:**
- [ ] CDC event từ DB → Bronze trong < 30 giây.
- [ ] PII (phone, ID) không xuất hiện dạng plaintext ở bất kỳ tầng nào trên data lake.
- [ ] Late-arriving event không ghi đè dữ liệu mới hơn.
- [ ] `RESTORE` rollback thành công trong < 10 giây.
