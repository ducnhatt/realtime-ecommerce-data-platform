# Silver transformation regression gate

## Mục tiêu

Gate này kiểm tra transformation thuần từ Bronze schema sang `ecommerce-silver-v1`. Nó chưa đọc hoặc ghi MinIO, vì vậy lỗi mapping/schema được tách khỏi S3A, Parquet và chiến lược ghi vật lý.

## Cơ chế của bounded Bronze-to-Silver job

`spark/jobs/recovery/bronze_to_silver_batch.py` là batch full-rebuild, không phải Structured Streaming và không có checkpoint:

1. Tạo Spark session theo source timezone và cấu hình S3A để kết nối MinIO.
2. Đọc snapshot hiện có tại `s3a://bronze/ecommerce_reviews` bằng Parquet rồi cache `MEMORY_AND_DISK`, vì cùng DataFrame được dùng cho nhiều validation action và transformation.
3. Chạy pre-write gate trên Bronze: input phải khác rỗng; chỉ có VALID/WARNING; validation errors rỗng; contract version đúng; Kafka topic/partition/offset không null và không trùng.
4. Gọi reusable transform trong `spark/pipeline/silver.py`: filter business-eligible row, flatten 32 payload fields, đổi snake_case, cast rating/date/timestamp/decimal, giữ warning và lineage, gắn Silver schema version, loại duplicate kỹ thuật theo Kafka lineage.
5. Validate logical Silver trước khi ghi: count và distinct lineage phải bằng Bronze; required column không null; status/version đúng; `total_amount = unit_price × quantity` sau decimal cast.
6. `repartition(SILVER_OUTPUT_PARTITIONS, "ingest_date")`, mặc định là một Spark output partition cho môi trường local.
7. Ghi Snappy Parquet với `mode("overwrite")`, partition vật lý theo `silver_schema_version/ingest_date` tại `s3a://silver/ecommerce_reviews`.
8. Đọc lại chính output vật lý từ MinIO và chạy lại Silver validation; sau đó đếm Parquet file đệ quy. Chỉ in `BRONZE_TO_SILVER_BATCH_OK` khi toàn bộ read-back gate đạt.
9. Unpersist các DataFrame và dừng Spark trong `finally`.

`output_partitions=1` là số Spark partition trước khi ghi, không có nghĩa toàn dataset chỉ có một file: `partitionBy` vẫn tách theo từng `ingest_date`, nên hai ngày hiện tại tạo hai Parquet file.

`overwrite` làm lần chạy lại không append thành 612 row và phù hợp bounded rebuild từ Bronze. Trade-off là Parquet overwrite không có atomic transaction: nếu job chết giữa lúc thay output, Silver có thể tạm thời thiếu; recovery là chạy lại từ Bronze. Job cũng chưa xử lý incremental arrival và không bảo vệ khỏi Bronze thay đổi đồng thời trong lúc batch đang chạy.

Ba input fixture gồm một `VALID`, một `WARNING` và một bản lặp kỹ thuật của VALID dùng cùng Kafka lineage. Expected output là hai row lineage duy nhất.

## Lệnh người dùng chạy

Đảm bảo Spark master/worker đang chạy:

```powershell
docker compose --profile streaming up -d spark-master spark-worker
docker compose --profile streaming ps spark-master spark-worker
```

Chạy regression gate:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/checks/contracts/validate_silver_transform.py
```

Gate này không cần Kafka/S3A package vì chỉ xử lý DataFrame fixture trong Spark.

Marker thành công:

```text
SILVER_TRANSFORM_CONTRACT_OK ... input_rows=3 output_rows=2 distinct_lineage=2 statuses=VALID,WARNING schema_version=ecommerce-silver-v1
```

## Runtime result — transformation contract đạt

Application `app-20260924182014-0000` đã trả đúng marker trên. Transformation contract gate hoàn thành: ba fixture tạo hai Silver lineage duy nhất, cả VALID/WARNING và schema version đều đúng.

## Cổng kế tiếp — bounded physical write

Khởi động MinIO và Spark nếu cần:

```powershell
docker compose --profile streaming up -d minio spark-master spark-worker
docker compose --profile streaming ps minio spark-master spark-worker
```

Chạy bounded Bronze-to-Silver write:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/jobs/recovery/bronze_to_silver_batch.py
```

Marker mong đợi tại baseline hiện tại:

```text
BRONZE_TO_SILVER_BATCH_OK ... bronze_count=306 silver_count=306 distinct_lineage=306 ... schema_version=ecommerce-silver-v1
```

Runtime application `app-20260924182807-0001` đã đạt đúng baseline:

```text
bronze_count=306 silver_count=306 distinct_lineage=306 parquet_files=2 output_partitions=1
```

Hai Parquet file tương ứng physical output theo các ingestion-date partition hiện có. Read-back contract trong writer đạt; warning SLF4J binding và truncated plan chỉ là warning quan sát/logging, không phải lỗi dữ liệu.

## Cổng kế tiếp — independent read-only reconciliation

Giữ Bronze ổn định rồi chạy:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/checks/reconciliation/reconcile_bronze_silver.py
```

Verifier độc lập dựng lại expected Silver từ Bronze nhưng chỉ đọc output vật lý. Nó kiểm tra count, schema type, lineage anti-join hai chiều và hash toàn bộ Silver row, cùng status/version/amount invariant và physical file count.

Marker mong đợi:

```text
BRONZE_SILVER_RECONCILIATION_OK ... bronze_count=306 silver_count=306 distinct_lineage=306 missing=0 extra=0 payload_mismatches=0 schema_mismatches=0 ...
```

Runtime application `app-20260924202938-0002` đã đạt:

```text
bronze_count=306 silver_count=306 distinct_lineage=306
valid_count=67 warning_count=239
missing=0 extra=0 payload_mismatches=0 schema_mismatches=0
ingest_dates=2 parquet_files=2
```

Correctness gate Bronze → Silver hoàn thành. Cổng cuối của Silver v1 là chạy lại bounded writer trên cùng Bronze snapshot rồi reconciliation lại; kết quả phải vẫn là 306 row/306 lineage, không missing/extra/mismatch và không tăng số file do append.

## Runtime result — no-change rebuild đạt

Application `app-20260924203215-0003` chạy lại full-overwrite trên cùng Bronze snapshot và vẫn trả:

```text
bronze_count=306 silver_count=306 distinct_lineage=306 parquet_files=2
```

Ngay sau đó read-only reconciliation `app-20260924203450-0004` tiếp tục đạt:

```text
bronze_count=306 silver_count=306 distinct_lineage=306
missing=0 extra=0 payload_mismatches=0 schema_mismatches=0
ingest_dates=2 parquet_files=2
```

Silver v1 đã chứng minh full rebuild không append trùng, output có thể tái tạo từ Bronze và physical content vẫn đúng sau rerun. File name vật lý có thể thay đổi qua overwrite nhưng logical dataset, lineage và file count ổn định.

Job preflight Bronze, transform, full-overwrite riêng prefix Silver rồi đọc lại để kiểm tra count, uniqueness, required null, decimal invariant và version. `overwrite` giúp rerun không append trùng, nhưng Parquet không có atomic transaction: nếu job chết giữa lúc ghi, Silver có thể tạm thời thiếu và phải rebuild từ Bronze. Đây là trade-off local v1; Delta Lake là hướng nâng cấp nếu cần commit nguyên tử hoặc incremental MERGE.

## Gate kiểm chứng

- Duplicate cùng Kafka lineage bị loại đúng một lần.
- VALID và WARNING đều đi vào Silver; warning flags được giữ.
- `reviewer_name = null` hợp lệ vẫn được bảo toàn.
- Rating, timestamp, date, decimal và Kafka offset có đúng type.
- `total_amount = unit_price × quantity` vẫn đúng sau decimal cast.
- Không có `payload`, `raw_payload` hoặc `validation_errors` trong Silver output.
- Thứ tự và tập cột khớp contract v1.

Sau khi marker đạt mới viết bounded MinIO Bronze-to-Silver job và physical reconciliation.
