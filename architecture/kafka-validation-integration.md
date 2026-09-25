# Bounded Kafka + contract validation integration

Ngày: 2026-09-24. Trạng thái: runtime gate đã đạt.

## 1. Bước này chứng minh điều gì?

Hai checkpoint trước chỉ chứng minh riêng lẻ:

- Spark đọc được bytes và metadata từ Kafka.
- Validator phân loại đúng sáu fixture được kiểm soát.

Bước này ghép hai ranh giới trong cùng một Spark application:

```text
Kafka snapshot
  -> raw payload + topic/partition/offset/timestamp
  -> parse 32-field contract
  -> VALID / WARNING / INVALID
  -> reconcile count + lineage + rule metrics
```

Mục tiêu là phát hiện assumption gap giữa payload thật của `create_data.py` và contract/validator. Đây vẫn là batch integration gate, chưa phải streaming pipeline.

## 2. Quyết định và trade-off

**Bounded read `earliest` → `latest`:** Spark chụp ending offsets khi job bắt đầu rồi tự kết thúc. Kết quả có phạm vi hữu hạn, dễ kiểm tra và không cần checkpoint. Đổi lại, event được producer ghi sau thời điểm snapshot có thể không thuộc lần chạy này.

**`failOnDataLoss=true`:** nếu offset đã bị retention xóa trong lúc đọc, job fail thay vì âm thầm bỏ record. Điều này ưu tiên correctness; một topic retention chỉ một ngày khiến test dễ thất bại nếu để input quá cũ.

**Lineage kỹ thuật:** `(kafka_topic, kafka_partition, kafka_offset)` phải duy nhất và số output phải bằng số input. Điều này kiểm tra pipeline không làm mất/nhân dòng; nó không phát hiện producer gửi cùng business payload hai lần ở hai offset khác nhau.

**Cache input và validated output:** nhiều action kiểm tra count/aggregate tái sử dụng cùng snapshot đã materialize. Đổi lại dùng thêm executor memory; phù hợp dataset local nhỏ nhưng cần xem lại cho volume lớn.

**Không ghi MinIO:** failure chỉ nằm trong Kafka connector, parse và validation. Kết quả đạt chưa chứng minh S3A, checkpoint, sink idempotency hoặc recovery.

## 3. Tư duy hệ thống

- **Boundary:** Kafka là stateful source; validation là stateless transformation; console output là bằng chứng tạm thời.
- **State:** Kafka giữ message/offset. Spark cache chỉ sống trong application và được giải phóng khi job kết thúc. Không tạo consumer checkpoint hay object mới.
- **Correctness invariants:** input > 0, input = output, lineage duy nhất, tổng status = output, INVALID phải có error, WARNING phải có warning, VALID không được có error/warning.
- **Failure mode:** broker/connector lỗi làm job fail; malformed payload trở thành INVALID thay vì làm application crash; empty topic làm test fail để tránh false positive.
- **Scaling:** 10 Kafka partition cung cấp tối đa 10 input task, nhưng worker hiện có 2 core nên chỉ khoảng hai task chạy đồng thời; phần còn lại chạy theo lượt.
- **Observability:** in count theo status/rule và lineage sample, không in raw payload vì có `reviewerName` và log volume lớn.
- **Time semantics:** Spark dùng `SOURCE_TIME_ZONE=Asia/Ho_Chi_Minh` để đối chiếu epoch với chuỗi ngày đúng như generator hiện tại.

## 4. Lệnh người dùng chạy

Job cần Kafka, Spark master và worker. MinIO không cần cho gate này.

```powershell
docker compose --profile streaming up -d kafka spark-master spark-worker
docker compose --profile streaming ps kafka spark-master spark-worker
```

Nếu topic không còn message do retention một ngày, chạy producer trong terminal khác một lúc rồi dừng bằng `Ctrl+C`:

```powershell
python -m create_data --mode realtime
```

Chạy integration job bằng một dòng PowerShell:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 /opt/spark-apps/checks/contracts/validate_kafka_snapshot.py
```

`spark.jars.ivy=/tmp/.ivy2` là bắt buộc với image hiện tại vì HOME của user trong container trỏ tới `/nonexistent`; nếu bỏ cấu hình này Ivy không tạo được cache để resolve Kafka connector.

## 5. Tiêu chí đạt

Cuối output phải có marker dạng:

```text
KAFKA_VALIDATION_OK application_id=... bootstrap_servers=kafka:29092 topic=ecommerce_reviews source_time_zone=Asia/Ho_Chi_Minh message_count=... output_count=... distinct_lineage=... partitions_with_data=... valid=... warning=... invalid=...
```

Điều kiện bắt buộc:

- `message_count > 0`;
- `message_count = output_count = distinct_lineage`;
- `valid + warning + invalid = output_count`;
- process trả exit code 0.

Không yêu cầu `invalid=0`: `create_data.py` chủ động inject null vào một số required field, nên INVALID là kết quả business-data dự kiến. Cũng không yêu cầu đủ 10 partition có data nếu snapshot quá nhỏ; `partitions_with_data` chỉ là tín hiệu coverage.

## 6. Cách đọc kết quả

- Nhiều `E006_REQUIRED_NULL`: phù hợp chaos injection đã biết.
- `E001/E003/E004/E005` trên producer hiện tại: dấu hiệu payload/contract drift, cần điều tra.
- `E206_REVIEW_TIME_MISMATCH`: kiểm tra timezone/config trước khi kết luận source sai.
- W101/W102: cảnh báo business đã dự kiến, không phải lỗi pipeline.
- Không có marker hoặc exception: gate chưa đạt; không chuyển sang MinIO sink.

Khi gate đạt, bước kế tiếp là thiết kế physical Bronze/DLQ write và idempotency/checkpoint semantics.

## 7. Runtime checkpoint — đạt

Application `app-20260924084421-0001` trả:

```text
KAFKA_VALIDATION_OK application_id=app-20260924084421-0001 bootstrap_servers=kafka:29092 topic=ecommerce_reviews source_time_zone=Asia/Ho_Chi_Minh message_count=168 output_count=168 distinct_lineage=168 partitions_with_data=10 valid=27 warning=116 invalid=25
```

Đối soát:

- 168 input = 168 output = 168 Kafka lineage duy nhất;
- cả 10 partition đều có dữ liệu;
- 27 + 116 + 25 = 168;
- 25 INVALID đều là `E006_REQUIRED_NULL`, khớp chaos injection dự kiến khoảng 15%;
- W101 xuất hiện 110 lần, W102 xuất hiện 31 lần; rule count có thể overlap trên cùng record;
- không có E001/E003/E004/E005 hoặc cross-field error.

Cổng này chứng minh payload Kafka thật tương thích với contract/validator và transformation bảo toàn record trong bounded snapshot. Nó vẫn chưa chứng minh continuous processing, checkpoint recovery, S3A/MinIO write hoặc multi-sink idempotency.

## 8. Spark 4.0.4 regression checkpoint — đạt

Ngày 2026-09-25, application `app-20260925083222-0002` trả:

```text
KAFKA_VALIDATION_OK application_id=app-20260925083222-0002 bootstrap_servers=kafka:29092 topic=ecommerce_reviews source_time_zone=Asia/Ho_Chi_Minh message_count=27 output_count=27 distinct_lineage=27 partitions_with_data=8 valid=2 warning=18 invalid=7
```

Gate xác nhận connector `spark-sql-kafka-0-10_2.13:4.0.4`, internal listener, schema parser, validation rules và lineage hoạt động trên Spark 4.0.4. Chỉ 8/10 partition có dữ liệu là phân bố hợp lệ của snapshot nhỏ 27 event; invariant completeness dựa trên tổng input/output và lineage, không yêu cầu mọi partition đều có record trong mọi mẫu.
