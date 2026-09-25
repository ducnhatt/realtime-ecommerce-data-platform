# ClickHouse staging và dbt Gold design v1

Ngày chốt hướng: 2026-09-25. Trạng thái: **đã chọn ClickHouse-first; chưa triển khai runtime**.

Đây là thiết kế ban đầu cho project cá nhân ưu tiên dữ liệu tăng lớn và truy vấn OLAP. Engine, partition, sorting key, batch size và aggregate strategy phải được đo lại trong từng execution gate; không coi cấu hình local single-node là production sizing.

## 1. Vai trò trong kiến trúc

MinIO Silver vẫn là nguồn dữ liệu chuẩn để replay/backfill. ClickHouse là warehouse/analytical serving layer cho dbt và Power BI:

```text
MinIO Silver Parquet
        ↓ incremental load
ClickHouse staging
        ↓ dbt-clickhouse
ClickHouse Gold wide fact + aggregates
        ↓
Power BI
```

ClickHouse được chọn thay PostgreSQL vì mục tiêu mới ưu tiên scan, filter và aggregate trên dữ liệu tăng lớn. Đây là OLAP warehouse, không dùng làm transactional application database.

## 2. Grain và giới hạn business giữ nguyên

Một Gold fact row tương ứng một Silver event business-eligible. Lineage kỹ thuật là:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

Không deduplicate theo `order_id` hoặc payload hash. Source không có order event time, lifecycle hay doanh thu kế toán; Gold không tạo `order_date`, `revenue`, `net_revenue` hoặc thời điểm chuyển trạng thái.

Không tách review fact vì review/giao dịch hiện cùng grain 1–1. Chưa tạo data-quality fact vì Silver không chứa INVALID; muốn tính DLQ rate phải ingest Bronze/DLQ metrics riêng.

## 3. Vì sao vẫn có `staging` và `gold`

Đây là hai database logic trong cùng một ClickHouse server, không phải hai cluster/container:

- `staging`: biên kỹ thuật nhận Silver, giữ lineage/version/load metadata và giải quyết replay/dedup.
- `gold`: biên business, chứa fact đã chuẩn hóa nghĩa KPI và bảng aggregate phục vụ BI.
- `control`: load ledger, batch metrics và reconciliation results.

Tách lớp giúp lỗi load không trộn với lỗi business model; dbt có thể chạy lại từ staging mà không đọc lại MinIO. Trade-off là thêm storage, nhưng ClickHouse columnar compression và khả năng chỉ đọc cột cần thiết phù hợp với lớp phục vụ phân tích.

## 4. Physical model v1

### `staging.ecommerce_events_raw`

- Engine đề xuất: `ReplacingMergeTree(load_version)`.
- Partition: `toYYYYMM(ingest_date)`.
- `ORDER BY (kafka_topic, kafka_partition, kafka_offset)` để các bản replay cùng lineage có cùng replacing key.
- Thêm `event_key`, `load_batch_id`, `load_version`, `loaded_at`.
- Không đưa `reviewer_name`, `review_text`, `review_summary` vào warehouse mặc định.

`ORDER BY` của ClickHouse không phải unique constraint. Background merge chỉ loại phiên bản theo thời gian; do đó correctness không được phép dựa riêng vào việc “chờ merge”. Trong gate đầu, dbt đọc current-state qua `FINAL`. Nếu `FINAL` trở thành bottleneck khi dữ liệu lớn, chuyển current-state computation sang `AggregatingMergeTree` với `argMaxState/argMaxMerge` thay vì ép `OPTIMIZE FINAL` toàn bảng.

### Gold

```text
gold.fact_ecommerce_events
gold.agg_ecommerce_daily
gold.dim_date
```

ClickHouse Gold ưu tiên **wide fact**: category, sub-category, brand, region, city, channel, payment method/status và shipping service/status nằm trực tiếp trong fact dưới dạng `LowCardinality(String)` phù hợp. Cách này giảm join khi dashboard scan lượng lớn dữ liệu. `dim_date` giữ calendar/role-playing semantics; các dimension product/store chuẩn vẫn không được tạo vì source thiếu master-data stability.

`fact_ecommerce_events` dùng `MergeTree`, partition theo tháng ingestion và sorting key bắt đầu bằng các cột lọc phổ biến đã được đo. Lineage vẫn nằm cuối sorting key để audit. `agg_ecommerce_daily` là bảng aggregate theo ingestion/review date và các chiều chính; cổng đầu build bằng dbt partition replacement. Chỉ chuyển sang materialized view/AggregatingMergeTree sau khi chứng minh insert-time idempotency, vì materialized view có thể nhân đôi aggregate nếu raw input bị replay.

## 5. KPI chính thức

| KPI | Công thức | Diễn giải |
|---|---|---|
| `event_count` | `count()` | Event business-eligible |
| `quantity_sum` | `sum(quantity)` | Tổng số lượng giả lập |
| `gross_item_value` | `sum(total_amount)` | Tổng giá trị dòng hàng giả lập |
| `successful_labeled_value` | `sumIf(total_amount, payment_status = 'Thành công')` | Giá trị có nhãn thanh toán thành công, không phải doanh thu kế toán |
| `average_rating` | `avg(overall_rating)` | Rating trung bình |
| `helpful_ratio` | `sum(helpful_yes) / nullIf(sum(total_vote), 0)` | Tỷ lệ vote hữu ích có trọng số |
| `warning_count` | `countIf(validation_status = 'WARNING')` | Warning trong tập business-eligible |
| `warning_rate` | warning count / event count | Không phải DLQ rate |

Review date chỉ dùng phân tích review; ingestion date dùng arrival/pipeline time. Không gọi review date là ngày bán hàng.

## 6. Incremental load và idempotency

1. Spark `availableNow` tiếp tục dùng checkpoint riêng để khám phá Silver file mới.
2. Ưu tiên official Spark ClickHouse DataSource V2 connector và ghi theo batch lớn; runtime được chuẩn hóa về Spark 4.0.4/Scala 2.13 theo compatibility matrix, nhưng vẫn phải chạy compatibility spike trước khi chốt artifact connector.
3. Nếu connector chưa tương thích, fallback là distributed HTTP bulk insert theo Spark partition, không `collect()` dữ liệu lớn về driver.
4. Mỗi row có deterministic event key và monotonically increasing `load_version`; staging `ReplacingMergeTree` hấp thụ technical replay.
5. dbt build Gold từ current-state staging và chỉ rebuild/replace partition bị ảnh hưởng.
6. `control.silver_load_batches` ghi batch ID, file/input row, inserted row, duration và error để reconciliation.

Semantics v1 là **at-least-once delivery + eventual physical dedup + query/build-time correctness theo lineage**. Không tuyên bố distributed exactly-once. File checkpoint là hàng rào đầu, lineage/ReplacingMergeTree là hàng rào replay, còn reconciliation là điều kiện công nhận dữ liệu.

Không dùng `max(ingest_date)` làm watermark vì late file có thể mang ngày cũ. Không insert từng row; mục tiêu batch tối thiểu sẽ được đo, và async insert chỉ bật khi micro-batch thực tế quá nhỏ.

## 7. dbt và BI

- Dùng adapter Python `dbt-clickhouse` GA cho v1; không lấy dbt v2 beta làm nền bắt buộc.
- dbt models gồm source/current staging, Gold wide fact, daily aggregate, tests và documentation.
- Tests: lineage not-null, current-state unique, accepted status, range, amount invariant, Gold/staging reconciliation và aggregate reconciliation.
- Power BI kết nối ClickHouse qua connector/ODBC phù hợp; chỉ đọc Gold, không đọc raw staging mặc định.

## 8. Scale và vận hành local

- Single-node ClickHouse + named volume phù hợp học tập, không đại diện HA.
- Partition theo tháng để tránh quá nhiều partition; sorting key chỉ chốt sau query-pattern test.
- Theo dõi part count, insert batch size, merge backlog, query latency, compression ratio và memory.
- Tránh tạo nhiều small inserts/parts; không dùng `OPTIMIZE TABLE ... FINAL` như cron mặc định.
- MinIO vẫn giữ lịch sử bền vững; ClickHouse có thể rebuild từ Silver khi mất local volume.

## 9. Trade-off so với PostgreSQL

Đạt được:

- columnar scan/compression và vectorized OLAP;
- aggregate latency tốt hơn khi dữ liệu tăng lớn;
- wide fact, projections/materialized aggregates và Power BI serving trong cùng engine.

Chấp nhận:

- không có unique constraint/`ON CONFLICT` kiểu PostgreSQL;
- dedup và update semantics phức tạp hơn;
- background merge tạo eventual physical state;
- tuning partition/sorting key/batch size trở thành trách nhiệm thiết kế;
- container local tiêu thụ thêm RAM/disk và vẫn là single point of failure.

## 10. Thứ tự triển khai

1. Pin ClickHouse image/version, thêm profile `warehouse`, named volume, healthcheck và resource limit.
2. Test server/client, persistence và MinIO connectivity.
3. Viết DDL `control`, `staging`, current-state view và contract test.
4. Compatibility spike Spark 4.0.4/Scala 2.13 → ClickHouse; chốt connector/fallback.
5. Implement Silver → staging incremental load và replay/idempotency test.
6. Khởi tạo `dbt-clickhouse`, wide fact, daily aggregate và tests.
7. Reconcile Silver → staging current → Gold/KPI.
8. Chỉ sau khi các gate đạt mới kết nối Power BI.
