# Bronze/DLQ Structured Streaming runbook

Ngày: 2026-09-24. Trạng thái: implementation ready, chờ runtime.

## 1. Mục tiêu gate

Chạy application dài hạn đầu tiên của project:

```text
Kafka ecommerce_reviews
        ├─ query Bronze: VALID/WARNING → Parquet
        └─ query DLQ:    INVALID       → Parquet
```

Hai query có checkpoint riêng. Lần chạy đầu dùng `startingOffsets=earliest`; khi checkpoint đã tồn tại, Spark resume từ checkpoint và bỏ qua cấu hình starting offset.

## 2. State và đường dẫn

```text
s3a://bronze/ecommerce_reviews/contract_version=.../ingest_date=.../
s3a://dlq/ecommerce_reviews/contract_version=.../ingest_date=.../
s3a://checkpoints/kafka_to_bronze_v1/bronze/
s3a://checkpoints/kafka_to_bronze_v1/dlq/
```

`ingest_date` được suy ra từ Kafka timestamp, không từ processing-time hiện tại. Vì vậy retry quanh nửa đêm vẫn định tuyến cùng event vào cùng date partition.

Không xóa checkpoint nếu chỉ muốn restart/resume. Xóa checkpoint làm query mất lịch sử progress và có thể đọc lại Kafka từ `earliest`, tạo duplicate logical data.

## 3. Local capacity trade-off

- Trigger mỗi 10 giây.
- Tối đa 10.000 Kafka offset/query/trigger để bảo vệ laptop khi backlog tăng.
- Mỗi sink coalesce về một output partition/micro-batch để hạn chế small files ở volume thấp.
- Hai query đọc và validate độc lập, nên compute gần gấp đôi; đổi lại checkpoint/failure boundary tách rõ.

Không dùng cấu hình một output partition cho production throughput cao. Khi scale, đo batch duration và file size trước khi tăng partition.

## 4. Khởi tạo hạ tầng

Chạy init để tạo thêm bucket kỹ thuật `checkpoints`:

```powershell
docker compose --profile streaming up -d minio minio-init kafka spark-master spark-worker
docker compose --profile streaming ps -a minio minio-init kafka spark-master spark-worker
docker compose --profile streaming logs minio-init
```

Log init phải có đủ `bronze`, `silver`, `dlq`, `checkpoints`; `minio-init` kết thúc code 0.

## 5. Chạy streaming application

Chạy ở terminal riêng và giữ process hoạt động:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/jobs/streaming/kafka_to_bronze_dlq.py
```

Marker khởi động:

```text
BRONZE_DLQ_STREAMING_STARTED application_id=... topic=ecommerce_reviews ...
```

`STARTED` chỉ xác nhận hai query đã được submit, không xác nhận micro-batch đã ghi và commit. Job còn in marker sau mỗi batch hoàn tất:

```text
BRONZE_DLQ_BATCH_COMMITTED query=ecommerce_bronze_v1 batch_id=... spark_input_rows=... kafka_offset_delta=... end_offset=...
BRONZE_DLQ_BATCH_COMMITTED query=ecommerce_dlq_v1 batch_id=... spark_input_rows=... kafka_offset_delta=... end_offset=...
```

Chỉ dừng application sau khi đã thấy `BATCH_COMMITTED` cho cả hai query ở batch cần kiểm tra. `spark_input_rows` là `numInputRows` Spark báo cho trigger; khi Spark cung cấp start/end offset, `kafka_offset_delta` là số Kafka record mà checkpoint tiến qua. Nếu progress không cung cấp offset, marker phải báo `kafka_offset_delta=UNAVAILABLE` và `offset_observation=UNAVAILABLE`, không được biến thiếu telemetry thành số 0. Khi metric thiếu hoặc mâu thuẫn, dùng bounded Kafka snapshot cùng physical reconciliation để quyết định completeness. Đây không phải số row cuối cùng được route vào từng sink; Bronze và DLQ là hai query độc lập nên cả hai cùng đọc toàn bộ input rồi mới filter.

Đây chỉ là marker hai query đã start, chưa phải bằng chứng record đã commit.

## 6. Phát event mới

Ở terminal khác, chạy producer khoảng 15–30 giây rồi dừng bằng `Ctrl+C`:

```powershell
python -m create_data --mode realtime
```

Đợi thêm ít nhất một trigger interval trước khi kiểm tra MinIO UI. Phải thấy:

- Bronze có prefix `ecommerce_reviews/contract_version=.../ingest_date=...`;
- DLQ có prefix tương tự nếu batch có ít nhất một INVALID;
- Checkpoints có hai prefix `bronze` và `dlq`.

Không coi việc chỉ nhìn thấy file là cổng hoàn thành. Bước kế tiếp sẽ đọc lại hai sink để đối soát status, lineage và duplicate trước/sau restart.

## 7. Dừng an toàn

Dừng producer trước, đợi ít nhất 10 giây, sau đó `Ctrl+C` terminal Spark. Không chạy `docker compose down -v`; tùy chọn `-v` sẽ xóa Kafka/MinIO named volumes và toàn bộ state.

## 8. Điều chưa được tuyên bố

- Chưa chứng minh restart/resume.
- Chưa chứng minh không duplicate sau failure.
- Chưa chứng minh Bronze + DLQ đối soát đủ toàn bộ Kafka snapshot.
- Chưa tuyên bố exactly-once xuyên hai query.

## 9. Initial materialization checkpoint — đạt

MinIO UI và MinIO Client đã xác nhận:

```text
bronze/ecommerce_reviews/_spark_metadata/{0..4}
bronze/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=2026-09-23/*.parquet
bronze/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=2026-09-24/*.parquet

dlq/ecommerce_reviews/_spark_metadata/{0..4}
dlq/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=2026-09-23/*.parquet
dlq/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=2026-09-24/*.parquet
```

Mỗi query có checkpoint:

```text
metadata
sources/0/0
offsets/{0..4}
commits/{0..4}
```

Initial offsets của cả 10 partition đều bằng 0. Commit batch 4 của Bronze và DLQ cùng có end offsets:

```text
0:22, 1:20, 2:21, 3:23, 4:13,
5:24, 6:31, 7:29, 8:24, 9:24
```

Tổng end offsets là 231 record. Hai query đang cùng progress tại checkpoint quan sát. Điều này chứng minh initial materialization và checkpoint commit, nhưng chưa thay thế row-level reconciliation.

Bronze và DLQ mỗi bên có sáu Parquet file: batch đầu ghi hai date partition, bốn batch sau mỗi batch ghi một date partition. `coalesce(1)` giới hạn task output nhưng `partitionBy` vẫn có thể tạo một file cho mỗi partition value trong cùng batch.

## 10. Physical reconciliation gate

Trước khi chạy, dừng producer, đợi ít nhất một trigger interval rồi dừng streaming application bằng `Ctrl+C`. Kafka và hai sink phải ổn định trong suốt phép đối soát.

Chạy batch verifier:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/checks/reconciliation/reconcile_bronze_dlq.py
```

Job chỉ đọc Kafka/Parquet và kiểm tra:

- Kafka count bằng Bronze + DLQ count;
- lineage duy nhất trong từng sink, không overlap giữa hai sink;
- không thiếu/thừa lineage so với Kafka snapshot;
- SHA-256 của `raw_payload` không đổi;
- Bronze chỉ có VALID/WARNING và không có validation error;
- DLQ chỉ có INVALID và có validation error;
- `ingest_date` khớp Kafka timestamp và contract version đúng.

Marker đạt:

```text
BRONZE_DLQ_RECONCILIATION_OK application_id=... kafka_count=... bronze_count=... dlq_count=... sink_count=... distinct_lineage=... missing=0 extra=0 overlap=0 payload_mismatches=0 bronze_files=... dlq_files=...
```

Nếu Kafka retention đã xóa event cũ nhưng sink còn giữ chúng, `extra > 0` không tự chứng minh sink sai; nó cho biết không còn snapshot Kafka đầy đủ để thực hiện exact reconciliation. Với checkpoint hiện tại và retention một ngày, nên chạy gate ngay sau khi dừng stream.

### Runtime checkpoint — đạt

Application `app-20260924103733-0001` trả:

```text
BRONZE_DLQ_RECONCILIATION_OK application_id=app-20260924103733-0001 kafka_count=231 bronze_count=195 dlq_count=36 sink_count=231 distinct_lineage=231 missing=0 extra=0 overlap=0 payload_mismatches=0 bronze_files=6 dlq_files=6
```

Physical status gồm 35 VALID, 160 WARNING và 36 INVALID. VALID/WARNING cộng đúng 195 Bronze; INVALID khớp đúng 36 DLQ. Count, set lineage và payload hash đều reconciliation thành công.

Cảnh báo SLF4J binding và Spark plan truncation chỉ ảnh hưởng logging/display, không ảnh hưởng kết quả. Marker success chỉ được in sau khi toàn bộ assertion bằng 0.
