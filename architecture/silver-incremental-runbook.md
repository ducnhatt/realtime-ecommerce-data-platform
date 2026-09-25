# Incremental Bronze-to-Silver available-now runbook

## 1. Vai trò

Silver v1 có hai execution path tách biệt:

- `bronze_to_silver_batch.py`: full-rebuild theo yêu cầu cho bootstrap, recovery và regression; không chạy định kỳ.
- `bronze_to_silver_incremental.py`: operational micro-batch, chỉ khám phá Bronze file chưa được checkpoint ghi nhận, xử lý hết backlog rồi tự dừng.

Incremental job dùng Spark Structured Streaming file source với `trigger(availableNow=True)`. Nó không phải process chạy 24/7 và sau này phù hợp để Airflow gọi theo lịch.

## 2. Namespace an toàn cho migration

Job incremental ban đầu không ghi đè dataset full-rebuild đã kiểm chứng:

```text
source:     s3a://bronze/ecommerce_reviews
output:     s3a://silver/ecommerce_reviews_incremental_v1
checkpoint: s3a://checkpoints/bronze_to_silver_incremental_v1
```

Prefix mới cho phép initial catch-up toàn bộ 306 Bronze row mà không phải xóa Silver hiện tại. Chỉ sau khi initial reconciliation, no-input rerun và delta test đạt mới chọn incremental dataset làm nguồn cho ClickHouse/Gold. Không được dùng chung checkpoint cho hai output path.

## 3. Cơ chế

1. Spark đọc schema Parquet từ Bronze footer để cung cấp explicit schema cho streaming file source.
2. File source dùng checkpoint để ghi nhớ file Bronze đã xử lý.
3. `maxFilesPerTrigger=10` giới hạn số file mỗi micro-batch; `availableNow` vẫn chạy liên tiếp đến khi hết backlog.
4. Transformation dùng cùng Silver contract nhưng tắt global `dropDuplicates`: stateful dedup không watermark sẽ tăng state vô hạn. Uniqueness của Bronze Kafka lineage là upstream invariant; read-only reconciliation phát hiện duplicate xuyên batch.
5. Output append Snappy Parquet, partition theo `silver_schema_version/ingest_date` với hai output partition mặc định cho worker hai core.
6. File sink và checkpoint giữ execution progress; rerun không có Bronze file mới phải có `input_rows=0` và không tạo thêm business row.

Trade-off Parquet v1: checkpoint/output phải được coi là một cặp state. Nếu checkpoint mất nhưng output còn, chạy lại từ đầu có thể append duplicate. Delta/Iceberg là hướng nâng cấp khi cần transaction/MERGE mạnh hơn. Các micro-batch nhỏ cũng có thể tạo small files; compaction chỉ thêm sau khi đo file count/size thực tế.

## 4. Initial catch-up

Đảm bảo MinIO và Spark đang chạy, đồng thời không chạy full-rebuild vào incremental prefix:

```powershell
docker compose --profile streaming up -d minio spark-master spark-worker
docker compose --profile streaming ps minio spark-master spark-worker
```

Chạy incremental available-now:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/jobs/incremental/bronze_to_silver_incremental.py
```

Marker initial mong đợi khi Bronze vẫn là 306 row:

```text
BRONZE_TO_SILVER_INCREMENTAL_OK ... input_rows=306 non_empty_batches=... last_batch_id=...
```

`non_empty_batches` và số output file phụ thuộc cách 14 Bronze file được chia theo `maxFilesPerTrigger`; không yêu cầu đúng một batch hoặc hai file.

### Runtime result — initial catch-up đạt

Application `app-20260924214725-0005` trả:

```text
query_id=dd21912e-c871-46ab-90c0-2b6711c22d59
input_rows=306 non_empty_batches=2 last_batch_id=1
max_files_per_trigger=10 output_partitions=2
```

Checkpoint và incremental output đều được tạo trong namespace riêng. Hai non-empty batch là kết quả mong đợi khi 14 Bronze Parquet file được giới hạn tối đa 10 file mỗi trigger. Marker này chứng minh Spark đã xử lý hết backlog và tự dừng; correctness của output còn phải được đóng bằng reconciliation bên dưới.

## 5. Reconciliation incremental output

```powershell
docker compose --profile streaming exec -e SILVER_DATASET_PREFIX=ecommerce_reviews_incremental_v1 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/checks/reconciliation/reconcile_bronze_silver.py
```

Gate yêu cầu 306/306 lineage, `missing=extra=payload_mismatches=schema_mismatches=0`. Physical file count có thể lớn hơn full rebuild và được ghi lại làm baseline small-file measurement.

### Runtime result — initial reconciliation đạt

Application `app-20260924215237-0006` xác nhận:

```text
bronze_count=306 silver_count=306 distinct_lineage=306
valid_count=67 warning_count=239
missing=0 extra=0 payload_mismatches=0 schema_mismatches=0
ingest_dates=2 parquet_files=3
```

Incremental output khớp hoàn toàn với Bronze. Ba file thay vì hai file full-rebuild là kết quả của hai micro-batch và hai output partition; task/partition không có row không tạo file. Baseline small-file hiện tại là 3, chưa đủ bằng chứng để thêm compaction.

## 6. No-input rerun và delta gate

Sau initial reconciliation, chạy lại đúng incremental command mà không tạo Bronze file mới:

- cùng query ID, run ID mới;
- `input_rows=0`;
- reconciliation vẫn giữ 306 row và không duplicate.

Ở no-input run, Spark có thể không tạo progress batch mới; khi đó marker của wrapper có `last_batch_id=-1`. Tiêu chí đúng là cùng query ID, run ID mới, `input_rows=0`, `non_empty_batches=0`, sau đó reconciliation vẫn giữ 306 row và 3 Parquet file.

### Runtime result — no-input execution đạt

Application `app-20260924215925-0007` khôi phục cùng query ID `dd21912e-c871-46ab-90c0-2b6711c22d59`, tạo run ID mới và trả:

```text
input_rows=0 non_empty_batches=0 last_batch_id=2
```

Spark đã tạo progress batch 2 rỗng nên `last_batch_id=2`, thay vì trường hợp không có progress và trả `-1`. Cả hai biểu hiện đều hợp lệ; bằng chứng quan trọng là cùng logical query/checkpoint, run mới và không có input row.

### Runtime result — no-input reconciliation đạt

Application `app-20260924220301-0008` xác nhận Silver vẫn có đúng 306 row/306 lineage và 3 Parquet file; `missing=0`, `extra=0`, `payload_mismatches=0`, `schema_mismatches=0`. Như vậy empty batch không ghi thêm business row hoặc file, và checkpoint đã chống replay đúng trong phạm vi thử nghiệm này.

Delta gate sau đó mới bật producer + Bronze stream trong khoảng có kiểm soát, dừng source, chạy incremental available-now và reconciliation. Silver phải chỉ tăng đúng bằng Bronze delta.

### Runtime result — controlled delta đã vào durable layer

Sau baseline Kafka 356/Bronze 306/DLQ 50, producer tạo tổng cộng 27 event mới. Reconciliation `app-20260924221823-0010` xác nhận Kafka 383 = Bronze 326 + DLQ 57, gồm 71 VALID + 255 WARNING + 57 INVALID; `missing=0`, `extra=0`, `overlap=0`, `payload_mismatches=0`. Vì vậy delta vật lý là +20 Bronze và +7 DLQ. Silver incremental vẫn ở 306; lần tiếp theo phải catch-up đúng 20 Bronze row rồi reconciliation về Bronze = Silver = 326.

### Runtime result — controlled delta Silver đạt

Application `app-20260925015737-0000` khôi phục query ID `dd21912e-c871-46ab-90c0-2b6711c22d59` và xử lý đúng 20 Bronze row mới trong một non-empty batch. Reconciliation `app-20260925020457-0001` xác nhận Bronze 326 = Silver 326, 326 lineage duy nhất, 71 VALID + 255 WARNING; `missing=0`, `extra=0`, `payload_mismatches=0`, `schema_mismatches=0`. Output hiện trải trên 3 ingestion date và 4 Parquet file. Silver incremental đã đạt đủ initial catch-up, no-input/no-replay và controlled delta.
