# Silver data contract v1 — draft

Ngày thiết kế: 2026-09-25. Trạng thái: **đã duyệt thiết kế; transformation regression chờ runtime**.

## 1. Mục tiêu và grain

Dataset Silver `ecommerce_reviews` là giao diện dữ liệu sạch, có schema ổn định cho ClickHouse/dbt và các phép phân tích sau này.

- Một row Silver tương ứng đúng một row Bronze có `validation_status` là `VALID` hoặc `WARNING`.
- Grain vẫn là một quan sát giao dịch-review giả lập, một sản phẩm, append-only.
- `INVALID` chỉ nằm trong DLQ và không được đưa sang Silver.
- Không diễn giải `total_amount` thành doanh thu kế toán.
- Không dùng review time làm ngày đặt hàng.
- Không suy ra lịch sử thay đổi payment/shipping status từ một nhãn trạng thái duy nhất.

Baseline hiện tại dự kiến: Bronze `306` row → Silver `306` row, nếu không có input mới trong lúc chạy gate.

## 2. Quyết định format và physical layout

V1 dùng **Parquet** trên MinIO, chưa dùng Delta Lake.

```text
s3a://silver/ecommerce_reviews/
  silver_schema_version=ecommerce-silver-v1/
    ingest_date=YYYY-MM-DD/
      part-*.snappy.parquet
```

Chỉ partition theo `silver_schema_version` và `ingest_date`:

- version cô lập breaking schema change;
- ngày ingestion phù hợp incremental processing và dữ liệu realtime hiện có;
- không partition theo category/region/status vì cardinality và volume local thấp, dễ tạo nhiều file nhỏ;
- không partition theo review date vì đó không phải business event time của giao dịch.

Parquet được chọn vì đơn giản, tương thích Spark/ClickHouse loading và đủ cho source append-only hiện tại. Trade-off: không có transaction log, MERGE, time travel hoặc schema enforcement mạnh như Delta Lake. V1 phải bù bằng checkpoint/rebuild strategy và reconciliation riêng.

## 3. Schema nghiệp vụ chuẩn hóa

| Bronze payload | Silver column | Silver type | Null | Quy tắc |
|---|---|---|---|---|
| `reviewerID` | `reviewer_id` | string | Không | Giữ nguyên sau validation |
| `asin` | `asin` | string | Không | ID sản phẩm nguồn; không đổi nghĩa thành master product ID |
| `reviewerName` | `reviewer_name` | string | Có | Giữ null hợp lệ |
| `helpful` | `helpful` | array<int> | Không | Giữ để audit quan hệ với hai cột vote |
| `reviewText` | `review_text` | string | Không | Dữ liệu giả lập; không đưa vào Gold/dashboard mặc định |
| `overall` | `overall_rating` | tinyint | Không | Cast từ double sau khi đã xác nhận chỉ thuộc 1–5 |
| `summary` | `review_summary` | string | Không | Dữ liệu giả lập; không đưa vào Gold/dashboard mặc định |
| `unixReviewTime` | `review_timestamp` | timestamp | Không | Chuyển từ epoch seconds với source timezone `Asia/Ho_Chi_Minh` |
| `reviewTime` | `review_date` | date | Không | Parse `MM dd, yyyy`; validator đã xác nhận khớp epoch |
| `day_diff` | `day_diff` | int | Không | Giữ để truy vết; chưa dùng làm KPI chính |
| `helpful_yes` | `helpful_yes` | int | Không | Giữ nguyên |
| `total_vote` | `total_vote` | int | Không | Giữ nguyên |
| `productName` | `product_name` | string | Không | Giữ nguyên nội dung, chuẩn hóa tên cột |
| `category` | `category` | string | Không | Giữ nguyên enum Unicode |
| `sub_category` | `sub_category` | string | Không | Đã là snake_case |
| `brand` | `brand` | string | Không | Warning business không làm mất row |
| `unitPrice` | `unit_price` | decimal(18,2) | Không | Cast chính xác; không dùng double ở Silver |
| `orderID` | `order_id` | string | Không | Không coi là khóa dedup business ngoài contract hiện tại |
| `quantity` | `quantity` | int | Không | Giữ nguyên |
| `totalAmount` | `total_amount` | decimal(18,2) | Không | Không tính lại hoặc ghi đè giá trị nguồn |
| `storeID` | `store_id` | string | Không | Không dùng làm master dimension ổn định ở v1 |
| `storeName` | `store_name` | string | Không | Giữ nguyên |
| `city` | `city` | string | Không | Giữ nguyên enum Unicode |
| `region` | `region` | string | Không | Giữ nguyên enum Unicode |
| `storeType` | `store_type` | string | Không | Chuẩn hóa tên cột |
| `paymentID` | `payment_id` | string | Không | Giữ nguyên |
| `paymentMethod` | `payment_method` | string | Không | Chuẩn hóa tên cột |
| `paymentStatus` | `payment_status` | string | Không | Chỉ là nhãn tại record, không phải lifecycle |
| `shippingID` | `shipping_id` | string | Không | Giữ nguyên |
| `shippingMethod` | `shipping_method` | string | Không | Chuẩn hóa tên cột |
| `carrierName` | `carrier_name` | string | Không | Chuẩn hóa tên cột |
| `shippingStatus` | `shipping_status` | string | Không | Chỉ là nhãn tại record, không phải lifecycle |

Không tạo `order_date`, `revenue`, `net_revenue` hoặc lifecycle timestamp vì source v1 không cung cấp đủ semantics.

## 4. Metadata, quality và lineage

| Silver column | Type | Nguồn/ý nghĩa |
|---|---|---|
| `kafka_topic` | string | Topic nguồn |
| `kafka_partition` | int | Partition nguồn |
| `kafka_offset` | long | Offset nguồn |
| `kafka_timestamp` | timestamp | Kafka record timestamp |
| `ingested_at` | timestamp | Thời điểm Bronze validator xử lý record |
| `ingest_date` | date | Ngày từ Kafka timestamp, dùng partition |
| `validation_status` | string | Chỉ `VALID` hoặc `WARNING` |
| `validation_warnings` | array<string> | Warning IDs được bảo toàn |
| `source_contract_version` | string | Đổi tên từ Bronze `contract_version` |
| `silver_schema_version` | string | Hằng `ecommerce-silver-v1` |

`raw_payload` không được copy sang Silver vì Bronze đã giữ bản gốc để replay/audit. `validation_errors` không cần mang sang vì row có error đã vào DLQ; job Silver vẫn phải assert mảng error rỗng trước khi ghi.

Khóa lineage và khóa chống trùng kỹ thuật:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

Không deduplicate theo `order_id`, `payment_id`, `shipping_id` hoặc hash payload. Nếu producer gửi cùng business payload ở offset mới, v1 coi đó là event mới vì source chưa có `event_id`.

## 5. Quy tắc transform và quality gate

Job phải thực hiện theo thứ tự:

1. Đọc chỉ prefix Bronze của `ecommerce_reviews`.
2. Assert mọi row có status `VALID/WARNING`, contract version được hỗ trợ và lineage không null.
3. Flatten `payload`, đổi tên cột và cast theo schema ở trên.
4. Tạo `review_timestamp`, `review_date`, `source_contract_version` và `silver_schema_version`.
5. Không trim, translate hoặc thay thế các giá trị Unicode business trong v1.
6. Assert lại `total_amount = unit_price × quantity` sau khi cast decimal; nếu sai thì fail job, không sửa âm thầm và không tự route sang DLQ ở tầng Silver.
7. Deduplicate kỹ thuật theo Kafka lineage trong input snapshot.
8. Ghi Parquet và chạy reconciliation trước khi công nhận output.

Các bất biến phải đạt:

```text
silver_count = bronze_VALID + bronze_WARNING
silver_distinct_lineage = silver_count
silver_duplicate_lineage = 0
silver_invalid_status = 0
silver_null_required = 0
silver_amount_mismatch = 0
```

Ngoài count, verifier phải anti-join lineage hai chiều giữa Bronze và Silver để phát hiện missing/extra, đồng thời so các trường chuẩn hóa quan trọng thay vì chỉ dựa vào tổng row.

## 6. Execution strategy v1

Cổng đầu tiên dùng **bounded batch snapshot**, chưa dùng Structured Streaming:

- dễ đóng băng input và chứng minh transformation đúng;
- dễ đối soát toàn bộ Bronze/Silver;
- tránh trộn lỗi schema/transform với checkpoint và concurrent arrival ngay từ lần đầu.

Sau khi batch gate đạt mới quyết định incremental mode. Với dữ liệu append-only có hai lựa chọn:

- Spark file streaming + checkpoint: độ trễ thấp hơn nhưng phụ thuộc file discovery/checkpoint và vẫn tạo small files;
- batch theo `ingest_date` + orchestration: đơn giản, dễ compact và hợp với Gold/BI, nhưng latency cao hơn.

V1 chưa chọn cách incremental trước khi đo số file, volume và nhu cầu freshness thực tế.

## 7. Small-file strategy

Dataset hiện chỉ vài trăm row nên không dùng số partition Kafka để quyết định số file Silver. Initial gate đặt mục tiêu ít file cho mỗi `ingest_date` và ghi nhận đồng thời:

- số input/output row;
- số input Bronze file và output Silver file;
- thời gian xử lý;
- kích thước output.

Không hard-code một file vĩnh viễn. Khi volume tăng, target file size hợp lý hơn số file cố định; compaction định kỳ chỉ được thêm sau khi có bằng chứng small-file problem.

## 8. Security và dữ liệu text

`reviewer_name`, `review_text` và `review_summary` được giữ ở Silver vì dữ liệu hiện tại là synthetic và contract muốn bảo toàn khả năng phân tích review. Chúng không được đưa vào Gold/dashboard mặc định. Nếu thay nguồn bằng dữ liệu thật, phải xem đây là dữ liệu cần phân loại, masking hoặc tách quyền truy cập trước khi tái sử dụng contract này.

## 9. Điều kiện duyệt trước implementation

Trước khi viết job cần đồng ý bốn quyết định:

1. Parquet và bounded batch cho gate đầu tiên.
2. Giữ đủ 32 nội dung nguồn sau chuẩn hóa, bỏ `raw_payload` khỏi Silver.
3. Kafka lineage là dedup key kỹ thuật duy nhất.
4. Partition theo `silver_schema_version/ingest_date`, chưa thêm category/region/status.

Bốn quyết định trên đã được người dùng duyệt ngày 2026-09-25. Transformation dùng lại được đặt tại `spark/pipeline/silver.py`; regression gate chưa ghi storage được mô tả trong [Silver transformation runbook](silver-transform-runbook.md).
