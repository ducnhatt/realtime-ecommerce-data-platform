# Checkpoint restart/resume test

Ngày: 2026-09-24. Trạng thái: test plan ready, chờ runtime.

## 1. Mục tiêu

Chứng minh hai query dùng checkpoint hiện có để tiếp tục từ Kafka offsets đã commit, thay vì đọc lại từ `earliest` sau khi Spark application restart.

Baseline đã đối soát:

```text
Kafka = Bronze + DLQ = 231 record
Bronze files = 6
DLQ files = 6
Checkpoint committed batch = 4
```

Test chia hai pha để tách hai câu hỏi:

1. Restart khi không có event mới có replay dữ liệu cũ không?
2. Sau restart, event mới có được append đúng một lần không?

## 2. Query identity

Spark Structured Streaming có:

- `query_id`: logical query identity, được khôi phục từ checkpoint và phải giữ nguyên qua restart;
- `run_id`: identity của một lần chạy, phải thay đổi sau mỗi restart;
- Spark `application_id`: cũng thay đổi khi submit application mới.

Marker start đã được bổ sung cả `query_id` và `run_id`. Cùng query ID nhưng run ID mới là bằng chứng application đã nối lại cùng checkpoint, không tạo logical query mới.

## 3. Điều kiện an toàn

- Không xóa/đổi tên bucket `checkpoints`.
- Không đổi checkpoint path hoặc query output schema trong test.
- Không chạy `docker compose down -v`.
- Producer phải dừng khi bắt đầu pha A.
- Chỉ chạy một instance streaming application tại một thời điểm; hai driver dùng cùng checkpoint đồng thời là không hợp lệ.

## 4. Lệnh streaming dùng cho cả hai pha

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/jobs/streaming/kafka_to_bronze_dlq.py
```

## 5. Pha A — restart không có event mới

1. Xác nhận producer không chạy.
2. Start streaming application bằng lệnh trên.
3. Ghi lại marker `BRONZE_DLQ_STREAMING_STARTED`, đặc biệt query ID và run ID.
4. Chờ ít nhất 20 giây, tương đương hai trigger interval.
5. Dừng application bằng `Ctrl+C`.
6. Chạy lại reconciliation:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/checks/reconciliation/reconcile_bronze_dlq.py
```

Pha A đạt khi marker reconciliation vẫn có:

```text
kafka_count=231
sink_count=231
distinct_lineage=231
missing=0 extra=0 overlap=0 payload_mismatches=0
bronze_files=6 dlq_files=6
```

Nếu file/count tăng dù Kafka không có event mới, checkpoint/file-sink behavior cần điều tra trước pha B.

## 6. Pha B — restart và xử lý delta mới

1. Start lại streaming application; query ID phải giống pha A, run ID/application ID phải mới.
2. Ở terminal khác, chạy producer khoảng 15–30 giây:

```powershell
python -m create_data --mode realtime
```

3. Dừng producer bằng `Ctrl+C`.
4. Đợi ít nhất 10 giây để commit micro-batch cuối.
5. Dừng streaming application bằng `Ctrl+C`.
6. Chạy lại reconciliation bằng lệnh ở pha A.

Pha B đạt khi:

- `kafka_count > 231`;
- `kafka_count = bronze_count + dlq_count = sink_count = distinct_lineage`;
- `missing=0`, `extra=0`, `overlap=0`, `payload_mismatches=0`;
- số record tăng đúng theo Kafka delta, không cộng lại 231 record cũ;
- query ID giữ nguyên và run ID thay đổi.

Số file có thể tăng theo số micro-batch và date partition; file count không phải record count.

## 7. Điều test này chứng minh và không chứng minh

Nếu cả hai pha đạt, có thể nói checkpoint recovery và no-replay behavior đã được kiểm chứng trong shutdown/restart có kiểm soát trên môi trường local.

Chưa được gọi là exactly-once end-to-end hoặc disaster recovery vì chưa thử kill giữa lúc commit, mất MinIO/Kafka, corrupt checkpoint hay concurrent driver. Những failure injection này là gate nâng cao riêng.

## 8. Runtime observation — delta đạt, no-input control chưa hợp lệ

Restart marker:

```text
application_id=app-20260924110503-0002
bronze_query_id=296a9aae-e884-4cf6-9f5c-b629178ea412
bronze_run_id=85cb8fae-67c9-485a-b014-347574f8d053
dlq_query_id=25a0227a-c443-415d-8dd8-ee0e0fbe38dd
dlq_run_id=51559ff0-24ce-4027-805e-9b1d1c179784
```

Reconciliation `app-20260924112115-0003` trả:

```text
kafka_count=305 bronze_count=260 dlq_count=45
sink_count=305 distinct_lineage=305
missing=0 extra=0 overlap=0 payload_mismatches=0
bronze_files=11 dlq_files=11
```

So với baseline 231/195/36/6/6:

- Kafka tăng 74;
- Bronze tăng 65;
- DLQ tăng 9;
- VALID tăng 17, WARNING tăng 48, INVALID tăng 9;
- mỗi sink tăng 5 Parquet file.

Vì Kafka count đã tăng, đây là bằng chứng delta-processing đúng sau restart, không phải no-input pha A. Spark consumer không thể tự tạo Kafka message; source đã nhận thêm 74 event trong khoảng test.

## 9. Runtime observation — phát hiện backlog 29 record

Reconciliation tiếp theo `app-20260924112720-0004` thất bại với:

```text
Kafka=334, sinks=305
Kafka lineage missing from sinks=29
```

Diễn giải: Kafka nhận thêm 29 event sau khi streaming application đã dừng, còn Bronze/DLQ vẫn giữ snapshot 305 đã được đối soát trước đó. Đây là source/sink lag được verifier phát hiện đúng, không phải bằng chứng checkpoint replay hoặc duplicate. Kafka đang làm durable buffer: dữ liệu mới vẫn nằm trong topic để stream resume xử lý.

Hành động tiếp theo:

1. bảo đảm producer đã dừng;
2. chạy lại streaming application với nguyên checkpoint để consume backlog 29;
3. dừng stream sau khi đã qua ít nhất hai trigger;
4. chạy reconciliation và yêu cầu Kafka = sinks = 334, `missing=extra=overlap=payload_mismatches=0`;
5. lấy kết quả đạt đó làm baseline ổn định mới, sau đó restart thêm một lần khi producer vẫn dừng để thực hiện no-input control thật sự.

Không dùng 305 làm baseline no-input nữa vì source đã thay đổi lên 334.

## 10. Runtime observation — checkpoint identity khôi phục, catch-up chưa commit

Streaming application `app-20260924113040-0005` khởi động với:

```text
bronze_query_id=296a9aae-e884-4cf6-9f5c-b629178ea412
bronze_run_id=e49fe8b1-3518-4296-ac49-5fc0a9420206
dlq_query_id=25a0227a-c443-415d-8dd8-ee0e0fbe38dd
dlq_run_id=4b6962e0-5318-4e5d-8586-8e0b80030bc9
```

Hai query ID giống run trước trong khi run ID thay đổi. Điều này xác nhận Spark đã mở lại đúng hai logical query từ checkpoint cũ và tạo execution mới.

Tuy nhiên reconciliation `app-20260924113120-0006` vẫn trả Kafka 334, sinks 305 và `missing=29`. Marker `BRONZE_DLQ_STREAMING_STARTED` được in ngay sau `.start()`, nên chỉ chứng minh query đã được submit; nó không chứng minh micro-batch backlog đã commit. Run này vì vậy xác nhận checkpoint identity recovery nhưng chưa xác nhận catch-up completion.

Ở lần chạy tiếp theo, không dừng theo thời gian chờ cố định. Job đã được bổ sung marker `BRONZE_DLQ_BATCH_COMMITTED`; giữ producer dừng và chỉ dừng stream sau khi thấy marker này cho cả `ecommerce_bronze_v1` và `ecommerce_dlq_v1` ở batch catch-up. Có thể kiểm chứng chéo bằng checkpoint `commits`/`offsets` của cả hai query. Sau đó reconciliation phải đạt Kafka = sinks = 334 trước khi chạy no-input control.

## 11. Runtime result — catch-up đạt

Application `app-20260924114510-0007` phục hồi cùng query ID, tạo run ID mới và hoàn thành:

```text
batch_id=10 input_rows=29  # cả Bronze và DLQ query
batch_id=11 input_rows=0   # cả Bronze và DLQ query
```

End offsets của 10 partition cộng lại bằng 334. Batch 11 không có input mới và giữ nguyên end offsets, xác nhận backlog đã được xử lý hết trước khi dừng.

Reconciliation `app-20260924114655-0008` trả:

```text
kafka_count=334 bronze_count=286 dlq_count=48
sink_count=334 distinct_lineage=334
missing=0 extra=0 overlap=0 payload_mismatches=0
bronze_files=12 dlq_files=12
```

Physical status gồm 61 VALID, 225 WARNING và 48 INVALID. Catch-up/recovery đạt. Baseline ổn định mới là `334/286/48/12/12`; còn chạy no-input control để chứng minh restart tiếp theo không replay hoặc tạo file mới khi Kafka không đổi.

## 12. Runtime result — no-input/no-replay đạt

Sau một restart có kiểm soát khi producer dừng, reconciliation `app-20260924115406-0010` giữ nguyên baseline:

```text
kafka_count=334 bronze_count=286 dlq_count=48
sink_count=334 distinct_lineage=334
missing=0 extra=0 overlap=0 payload_mismatches=0
bronze_files=12 dlq_files=12
```

Không có row mới, lineage mới hoặc Parquet file mới. Hai pha delta catch-up và no-input control đều đạt; checkpoint recovery/no-replay gate được đóng cho shutdown/restart có kiểm soát trên môi trường local. Kết quả này không mở rộng thành tuyên bố exactly-once end-to-end hoặc disaster recovery.
