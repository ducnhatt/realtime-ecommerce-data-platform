# Bronze/DLQ physical storage design v1

Ngày: 2026-09-24. Trạng thái: thiết kế ban đầu; S3A connectivity gate đã đạt.

## 1. Mục tiêu và ranh giới

Đưa output đã validate từ Spark vào object storage local theo hai tuyến:

```text
VALID / WARNING ──> Bronze Parquet
INVALID         ──> DLQ Parquet
```

Thiết kế này chỉ dành cho project local. MinIO Community legacy mô phỏng S3 API; không đại diện topology production hoặc high availability.

## 2. Quyết định format

Chọn Parquet cho v1 vì:

- lưu schema và kiểu dữ liệu;
- columnar, phù hợp đọc phân tích và Silver downstream;
- Spark hỗ trợ trực tiếp, không thêm table-format dependency;
- đủ để học partition, checkpoint, compaction và replay.

Trade-off: Parquet không cung cấp transaction log, merge/upsert hoặc schema evolution có kiểm soát như Delta Lake. Nếu project cần update, CDC hoặc commit nhiều bảng nguyên tử thì xem xét Delta ở phiên bản sau.

## 3. Logical content

### Bronze

Chỉ chứa VALID và WARNING:

- typed payload 32 trường;
- `raw_payload` để audit/reprocess;
- `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`;
- `ingested_at`, `contract_version`;
- `validation_status`, `validation_errors`, `validation_warnings`.

### DLQ

Chỉ chứa INVALID:

- `raw_payload` là nguồn replay chính;
- Kafka lineage và ingestion metadata;
- error/warning IDs, missing/unexpected fields;
- parsed columns có thể null/partial và không được dùng làm nguồn replay chính.

## 4. Object layout v1

Định hướng layout:

```text
s3a://bronze/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=YYYY-MM-DD/
s3a://dlq/ecommerce_reviews/contract_version=ecommerce-event-v1/ingest_date=YYYY-MM-DD/
s3a://checkpoints/kafka_to_bronze_v1/bronze/
s3a://checkpoints/kafka_to_bronze_v1/dlq/
```

Chỉ partition theo ngày ingestion ở v1. Không partition theo `orderID`, category hoặc partition/offset vì cardinality/small-file cost. Với tốc độ producer thấp, partition theo giờ cũng chưa có lợi. Small files vẫn có thể xuất hiện theo micro-batch và sẽ cần trigger interval/compaction được đo thực tế.

Bucket `checkpoints` phải được tạo ở bước triển khai streaming, tách khỏi data buckets vì checkpoint có lifecycle và quyền ghi khác dữ liệu business.

## 5. Multi-sink và recovery

Chọn hai streaming query trong cùng application:

- query Bronze có checkpoint riêng;
- query DLQ có checkpoint riêng;
- nếu một query fail, application fail-fast và dừng query còn lại;
- khi restart, mỗi query resume từ checkpoint của chính nó và phần chậm hơn bắt kịp.

Lý do không dùng một `foreachBatch` ghi tuần tự hai Parquet sink: nếu Bronze write thành công nhưng DLQ write thất bại, Spark retry callback và có thể tạo file Bronze trùng vì hai write không có transaction chung.

Trade-off của hai query là Kafka được đọc và validation được tính hai lần, đồng thời hai sink có thể lệch tạm thời. Với volume local thấp, chấp nhận compute này để recovery boundary rõ hơn. Phải theo dõi progress/offset của cả hai query và kiểm thử restart trước khi phát biểu delivery guarantee.

Không tuyên bố exactly-once chỉ vì có checkpoint. Object-store committer, query checkpoint, retry và duplicate behavior phải được kiểm chứng runtime.

## 6. S3A dependency và MinIO configuration

Package cloud phải cùng release với Spark runtime. Với runtime 4.0.4, chọn:

```text
org.apache.spark:spark-hadoop-cloud_2.13:4.0.4
```

Package này gom cloud connector/dependencies tương thích với Spark release. Không tự chọn AWS SDK version khác hoặc trộn `hadoop-aws` khác version với Hadoop client.

MinIO local cần:

- endpoint nội bộ `http://minio:9000`;
- path-style access;
- SSL tắt vì endpoint local dùng HTTP;
- simple credential provider từ environment variables;
- region ký request cố định `us-east-1`;
- không hard-code secret trong Python source.

## 7. Gate đầu tiên: Spark ↔ MinIO S3A

Trước streaming, `minio_s3a_smoke_test.py` sẽ:

1. tạo path UUID dưới `bronze/_connectivity_test`;
2. ghi 100 row thành hai Parquet file qua S3A;
3. đọc lại và đối soát count/sum/run ID;
4. xóa đúng path UUID vừa tạo;
5. xác nhận path không còn rồi mới in success marker.

Gate này kiểm tra dependency, DNS/network, authentication, bucket access, Parquet write/read và delete. Nó không kiểm tra Kafka, validator, streaming checkpoint hoặc multi-sink recovery.

Marker đạt:

```text
MINIO_S3A_SMOKE_TEST_OK application_id=... bucket=bronze row_count=100 id_sum=5050 parquet_files=... cleanup=true
```

Sau runtime thành công mới thêm bucket checkpoint và triển khai streaming Bronze/DLQ.

### Runtime checkpoint — đạt

Application `app-20260924090739-0000` trả:

```text
MINIO_S3A_SMOKE_TEST_OK application_id=app-20260924090739-0000 bucket=bronze row_count=100 id_sum=5050 parquet_files=2 cleanup=true
```

Spark đã ghi hai Parquet file, đọc lại đúng 100 row/tổng ID 5050 và xóa thành công path test UUID. Gate chứng minh S3A dependency, endpoint nội bộ, credentials, bucket permission và Parquet write/read/delete. Kiểm tra recursive sau đó cho thấy data path UUID đã mất nhưng còn directory marker 0 byte `bronze/_connectivity_test/`; đây là object-store marker vô hại, không phải Parquet data.

Cảnh báo thiếu SLF4J `StaticLoggerBinder` không làm I/O thất bại; nó cho biết một dependency không tìm thấy logging binding tương ứng. Ghi nhận đây là classpath/logging tech debt, chưa tự ý thêm binding vì thêm sai version có thể gây xung đột với logging stack của Spark.

### Spark 4.0.4 regression checkpoint — đạt

Sau khi chuyển runtime, application `app-20260925082819-0001` trả:

```text
MINIO_S3A_SMOKE_TEST_OK application_id=app-20260925082819-0001 bucket=bronze row_count=100 id_sum=5050 parquet_files=2 cleanup=true
```

Kết quả xác nhận package `spark-hadoop-cloud_2.13:4.0.4` tương thích với runtime Spark 4.0.4 trong phạm vi S3A/MinIO smoke test. Đây chưa phải bằng chứng cho Kafka connector hoặc pipeline Bronze/DLQ đầy đủ.

## 8. Tài liệu kỹ thuật tham chiếu

- [Apache Spark 4.0.4 — Integration with Cloud Infrastructures](https://spark.apache.org/docs/4.0.4/cloud-integration.html)
- [Apache Hadoop — S3A connector](https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/)
- [Apache Hadoop — S3A troubleshooting](https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)

Nguyên tắc dependency quan trọng: `hadoop-aws` phải khớp chính xác Hadoop client và không tự thay AWS SDK bằng version tùy ý. Vì vậy project ưu tiên module cloud cùng Spark release thay vì ghép JAR thủ công.
