# Spark-to-Kafka connectivity test

Ngày: 2026-09-24.

> Runtime checkpoint trong tài liệu này được tạo trên Spark 4.2.0. Dependency và tiêu chí chạy lại đã được cập nhật cho Spark 4.0.4; gate 4.0.4 vẫn đang chờ người dùng rerun.

## 1. Mục tiêu

Kiểm tra một ranh giới duy nhất: Spark có nạp được Kafka connector và đọc được message từ topic `ecommerce_reviews` qua Docker network hay không.

```text
Spark driver
    |
    | kafka:29092 (INTERNAL listener)
    v
Kafka topic ecommerce_reviews
    |
    v
Spark executor đọc partition/offset/value
```

Test này chưa parse JSON, chưa validation, chưa streaming liên tục và chưa ghi MinIO. Việc giới hạn phạm vi giúp phân biệt lỗi kết nối/connector với lỗi schema hoặc S3A ở các bước sau.

## 2. Vì sao dùng batch read?

Test đọc một snapshot offset từ `earliest` đến `latest` rồi tự kết thúc. So với `readStream`, batch read dễ xác định pass/fail, không cần checkpoint và không để lại process streaming phải dừng thủ công.

Trade-off: batch test chỉ chứng minh Spark đọc được Kafka tại thời điểm chạy; chưa chứng minh micro-batch tiếp tục nhận event mới hoặc phục hồi từ checkpoint.

## 3. Dependency

Spark image không đóng gói Kafka connector mặc định. Lệnh submit thêm đúng artifact tương ứng Spark 4.0.4 và Scala 2.13:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4
```

`--packages` phù hợp giai đoạn kiểm chứng vì đơn giản và tự giải dependency, nhưng lần đầu cần Internet/Maven Central và thời gian tải. Trước khi làm pipeline ổn định, dependency sẽ được đóng vào custom image hoặc cache có phiên bản cố định để startup có thể tái lập.

Không thêm thủ công `kafka-clients`; connector quản lý phiên bản transitive phù hợp.

## 4. Listener được sử dụng

- Producer trên Windows dùng `localhost:9092`.
- Spark trong Docker dùng `kafka:29092`.

Container không dùng `localhost:9092`, vì `localhost` bên trong Spark container là chính container Spark, không phải Kafka.

## 5. Sự cố Ivy cache đã xử lý

Lần submit đầu dừng trước khi chạy application vì Ivy mặc định cố ghi Kafka connector vào `/nonexistent/.ivy2.5.2`. Đây là home directory không ghi được của user trong Spark image, không phải lỗi Kafka.

Test được chạy lại với `spark.jars.ivy=/tmp/.ivy2`, cho Ivy một cache path ghi được. Connector sau đó resolve thành công.

## 6. Runtime checkpoint — đạt

Ngày 2026-09-23, người dùng xác nhận:

```text
SPARK_KAFKA_READ_OK application_id=app-20260923174010-0000 bootstrap_servers=kafka:29092 topic=ecommerce_reviews message_count=39 partitions_with_data=10
```

Ba record mẫu chứa đủ `topic`, `partition`, `offset`, `timestamp`, `message_key` và `raw_payload`. Kết quả chứng minh:

1. Kafka, master và worker hoạt động cùng Docker network.
2. Connector Spark 4.2.0/Scala 2.13 lịch sử đã được resolve thành công; connector 4.0.4 phải được xác minh lại.
3. Driver kết nối Kafka qua internal listener `kafka:29092`.
4. Executor đọc được 39 message phân bố trên cả 10 partition.

File smoke test đã được xóa sau khi xác minh. Source mount `spark/jobs:/opt/spark-apps` được giữ vì pipeline Kafka-to-Bronze thật sẽ dùng cùng đường dẫn. Ivy cache trong `/tmp` là cache dependency tạm trong container, không phải dữ liệu business hoặc named volume.
