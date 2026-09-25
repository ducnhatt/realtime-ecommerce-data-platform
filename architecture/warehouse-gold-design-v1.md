# PostgreSQL staging và dbt Gold design v1 — superseded

Ngày thiết kế: 2026-09-25. Trạng thái: **không được chọn; được thay thế bởi [ClickHouse Gold design v1](clickhouse-gold-design-v1.md)**.

Lý do thay đổi: người dùng ưu tiên OLAP trên dữ liệu tăng lớn. File này được giữ như decision history để thể hiện trade-off PostgreSQL/ClickHouse, không dùng làm implementation contract.

Đây là thiết kế ban đầu cho project cá nhân. Khi triển khai từng cổng, schema, index, batch size và materialization có thể được xem xét lại dựa trên số liệu runtime; mọi thay đổi semantics phải được ghi thành quyết định mới.

## 1. Mục tiêu và ranh giới

PostgreSQL là analytical serving store local thay cho Redshift. dbt Core biến staging thành mô hình Gold cho Power BI. MinIO Silver vẫn là nguồn dữ liệu chuẩn để replay/rebuild; PostgreSQL không thay thế data lake.

Nguồn v1 không có order event time, lifecycle trạng thái hoặc doanh thu kế toán. Gold chỉ cung cấp thống kê mô tả trên event giả lập và không tạo các trường `order_date`, `revenue`, `net_revenue` hay thời điểm chuyển trạng thái.

## 2. Grain được chốt

`gold.fact_ecommerce_events` có đúng một row cho một Silver row, được nhận diện bởi:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

Grain business là một quan sát giao dịch-review giả lập, một sản phẩm và các nhãn payment/shipping tại record. `order_id`, `payment_id` và `shipping_id` là degenerate identifiers để truy vết, không phải khóa dedup.

Không tách `fact_reviews`: review và giao dịch hiện luôn có quan hệ 1–1 trong cùng source record. Tách hai fact sẽ nhân đôi storage, test và join mà không tạo thêm grain phân tích.

Không tạo `fact_data_quality` trong cổng đầu: Silver không chứa INVALID nên không thể tính DLQ rate đầy đủ. Fact hiện giữ `validation_status` và warning; data-quality fact chỉ được thêm khi có load riêng từ Bronze/DLQ hoặc metrics pipeline.

## 3. PostgreSQL schemas

```text
control.silver_load_batches
staging.ecommerce_events

gold.fact_ecommerce_events
gold.dim_date
gold.dim_category
gold.dim_region
gold.dim_brand
gold.dim_channel
gold.dim_payment_method
gold.dim_shipping_service
```

### `control.silver_load_batches`

Ghi `query_id`, `batch_id`, thời gian bắt đầu/kết thúc, trạng thái, input row, inserted row, conflict row và error. Bảng này là operational ledger, không phải nguồn xác định row đã load; unique lineage ở staging mới là hàng rào idempotency cuối.

### `staging.ecommerce_events`

- Giữ các cột Silver cần cho Gold và lineage/version metadata.
- Primary/unique key: `(kafka_topic, kafka_partition, kafka_offset)`.
- Không load `reviewer_name`, `review_text`, `review_summary` hoặc mảng `helpful` vào warehouse mặc định.
- Giữ `reviewer_id` để đếm distinct trên dữ liệu synthetic; không đưa tên người review lên Gold/dashboard.
- Dùng kiểu PostgreSQL chính xác: `numeric(18,2)`, `smallint`, `integer`, `bigint`, `date`, `timestamptz`, `text`, `text[]`.

## 4. Mô hình Gold

### Fact

`gold.fact_ecommerce_events` giữ:

- lineage: event key cùng topic/partition/offset;
- role-playing date keys: `review_date_key`, `ingest_date_key`;
- foreign keys đến category, region, brand, channel, payment method và shipping service;
- degenerate IDs: order, payment, shipping, reviewer và ASIN;
- observed labels: `payment_status`, `shipping_status`, `validation_status`;
- measures: `quantity`, `unit_price`, `total_amount`, `overall_rating`, `helpful_yes`, `total_vote`;
- version/audit metadata cần thiết.

Không đưa `product_name`, `store_name` thành canonical dimension attributes vì generator không cung cấp master-data stability. Có thể giữ trong staging để audit, nhưng BI v1 không dựa vào chúng.

### Dimensions

- `dim_date`: calendar dùng hai vai trò review date và ingestion date. Review date chỉ phục vụ phân tích review; ingestion date phục vụ thời điểm pipeline nhận dữ liệu.
- `dim_category`: tổ hợp category/sub-category quan sát được.
- `dim_region`: tổ hợp region/city quan sát được.
- `dim_brand`: brand quan sát được; warning brand/category vẫn nằm ở fact.
- `dim_channel`: `store_type`, không dùng `store_id` làm master store.
- `dim_payment_method`: phương thức thanh toán; trạng thái nằm ở fact vì là nhãn của observation.
- `dim_shipping_service`: shipping method/carrier; trạng thái nằm ở fact.

Các dimension v1 là SCD0/deterministic observed values, không dùng SCD2. Source append-only không cung cấp event thay đổi master data và các ID product/store chưa ổn định; dựng lịch sử SCD2 lúc này sẽ tạo lịch sử giả.

Surrogate key được sinh xác định từ business attributes chuẩn hóa. `dim_date` dùng khóa số `YYYYMMDD`; các dimension còn lại dùng hash ổn định và có dbt test unique/not-null/relationship.

## 5. KPI chính thức

| KPI | Công thức | Diễn giải |
|---|---|---|
| `event_count` | `count(*)` | Số event business-eligible |
| `quantity_sum` | `sum(quantity)` | Tổng số lượng giả lập |
| `gross_item_value` | `sum(total_amount)` | Tổng giá trị dòng hàng giả lập |
| `successful_labeled_value` | `sum(total_amount) filter (where payment_status = 'Thành công')` | Giá trị có nhãn thanh toán thành công, không phải doanh thu kế toán |
| `average_rating` | `avg(overall_rating)` | Rating trung bình |
| `helpful_ratio` | `sum(helpful_yes) / nullif(sum(total_vote), 0)` | Tỷ lệ vote hữu ích có trọng số |
| `warning_count` | count status `WARNING` | Số event business-eligible có cảnh báo |
| `warning_rate` | warning count / event count | Tỷ lệ warning trong Silver/Gold, không phải DLQ rate |

Các KPI có thể cắt theo ingestion date, review date, category, region, brand, channel, payment method/status và shipping service/status. Không gọi trục review date là ngày bán hàng.

## 6. Incremental load Silver → PostgreSQL

1. Spark file stream đọc prefix Silver incremental bằng `availableNow` và checkpoint riêng `silver_to_postgres_v1`.
2. Mỗi micro-batch ghi staging bằng `INSERT ... ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING`.
3. Chỉ khi toàn bộ partition hoàn tất thì Spark commit batch checkpoint; partial failure được retry, row đã ghi được unique constraint bỏ qua.
4. Load batch được ghi vào `control.silver_load_batches` để quan sát và đối soát.
5. Sau load thành công, chạy `dbt build`: dimensions trước, fact sau, rồi tests.

Không dùng `max(ingest_date)` làm watermark vì file đến muộn có thể mang ngày cũ và bị bỏ qua. File checkpoint quyết định discovery; unique lineage trong PostgreSQL quyết định idempotency.

Semantics được tuyên bố là **at-least-once delivery + idempotent sink theo Kafka lineage**, không tuyên bố distributed exactly-once giữa MinIO, Spark và PostgreSQL.

## 7. dbt materialization v1

- Source: `staging.ecommerce_events`.
- Dimensions nhỏ được build dạng table từ tập giá trị distinct; chi phí full rebuild hiện thấp và đơn giản để kiểm chứng.
- Fact dùng incremental materialization với unique event key; schema change phải fail rõ ràng, không tự động nuốt cột.
- Build order bảo đảm dimensions hoàn thành trước fact; relationship tests chặn orphan foreign keys.
- Full refresh là công cụ recovery có chủ đích, không phải lịch vận hành mặc định.

## 8. Quality, reconciliation và security

Các cổng bắt buộc:

```text
staging_count = silver_count
staging_distinct_lineage = staging_count
fact_count = staging_count
missing_lineage = 0
extra_lineage = 0
dimension_orphan_count = 0
gross_item_value(staging) = gross_item_value(gold)
successful_labeled_value(staging) = successful_labeled_value(gold)
```

dbt tests tối thiểu: unique/not-null event key và natural lineage, accepted values cho status, relationships cho mọi foreign key, quantity/rating range và `total_amount = unit_price * quantity`.

PostgreSQL chỉ bind `127.0.0.1` trong môi trường local, dùng user riêng cho loader/dbt/BI ở giai đoạn hardening. Password lấy từ environment, không commit secret. PII/text không được đưa vào Gold mặc định.

## 9. Trade-off đã chấp nhận

- Star schema dễ dùng cho Power BI nhưng thêm bước dbt và reconciliation.
- Không có product/store dimension chuẩn làm giảm khả năng drill-down theo entity, đổi lại không tạo master data giả.
- Dimension full rebuild phù hợp volume local; khi cardinality tăng mới chuyển sang incremental snapshot/SCD.
- PostgreSQL phù hợp project cá nhân và BI local, không đại diện khả năng scale của cloud MPP warehouse.
- `ON CONFLICT DO NOTHING` bảo vệ replay; correction cùng lineage phải dùng quy trình rebuild rõ ràng thay vì âm thầm update dữ liệu đã công bố.

## 10. Thứ tự triển khai

1. PostgreSQL container, named volume, healthcheck và persistence test.
2. DDL cho `control` và `staging`, cùng contract test.
3. Spark incremental loader Silver → staging với retry/idempotency test.
4. Khởi tạo dbt project, source/staging tests.
5. Xây dimensions và fact.
6. Reconciliation KPI Silver/staging/Gold.
7. Chỉ sau khi các cổng trên đạt mới kết nối Power BI.
