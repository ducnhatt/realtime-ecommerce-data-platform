# Spark local runbook

Ngày: 2026-09-23.

> Các checkpoint ở cuối tài liệu là bằng chứng lịch sử trên Spark 4.2.0. Cấu hình hiện hành đã chuyển sang Spark 4.0.4 và chỉ được đánh dấu đạt lại sau khi rerun.

## 1. Phạm vi bước này

Bước này chỉ dựng và kiểm tra Spark Standalone runtime. Chưa đọc Kafka, chưa validate event và chưa ghi MinIO.

Topology local:

```text
spark-master:7077
       |
       | đăng ký worker, phân lịch task
       v
spark-worker: 2 cores, 2 GB
```

| Thành phần | Vai trò | UI |
|---|---|---|
| `spark-master` | Nhận application, phân lịch executor lên worker | `http://localhost:8080` |
| `spark-worker` | Cung cấp CPU/RAM và thực thi task | `http://localhost:8081` |

Master không xử lý dữ liệu thay worker. Một master và một worker trên cùng laptop không cung cấp HA hoặc khả năng chịu lỗi máy; topology này dùng để học submission, scheduling, executor và quan sát job.

## 2. Phiên bản và tài nguyên

- Image: `spark:4.0.4-scala2.13-java17-python3-ubuntu`.
- Spark 4.0.4, Scala 2.13, Java 17, Python 3.
- Worker: 2 core và 2 GB RAM dành cho Spark application.
- Master RPC: `7077`; Master UI: `8080`; Worker UI: `8081`.

Chọn bản image có Python vì pipeline sẽ viết bằng PySpark. Không dùng tag `latest` để tránh thay đổi môi trường ngoài ý muốn.

## 3. Tại sao chưa thêm job?

Việc tách runtime khỏi pipeline giúp xác định lỗi theo lớp:

1. Nếu worker không đăng ký được, lỗi nằm ở Spark/network/runtime.
2. Nếu runtime đạt nhưng job không đọc Kafka, kiểm tra connector và Kafka listener.
3. Nếu đọc Kafka được nhưng không ghi MinIO, kiểm tra S3A và credentials.
4. Nếu kết nối đều đạt nhưng record đi sai nhánh, kiểm tra validation/business rule.

Nếu thêm tất cả cùng lúc, một lỗi dependency có thể bị nhầm thành lỗi logic dữ liệu.

## 4. Lệnh người dùng tự chạy

Khởi động hai Spark service:

```powershell
docker compose --profile streaming up -d spark-master spark-worker
```

Image Spark khá lớn nên lần pull đầu có thể mất thời gian. Kiểm tra trạng thái:

```powershell
docker compose --profile streaming ps spark-master spark-worker
```

Xem log nếu service chưa healthy:

```powershell
docker compose --profile streaming logs spark-master spark-worker
```

Mở Master UI:

```text
http://localhost:8080
```

Kết quả mong đợi:

- Master và worker đều `healthy`.
- Master UI hiển thị URL `spark://spark-master:7077`.
- Khu vực Workers có đúng một worker ở trạng thái `ALIVE`.
- Tổng tài nguyên hiển thị 2 cores và khoảng 2 GB memory.

Worker UI có thể xem tại `http://localhost:8081`.

## 5. Cổng hoàn thành

Spark runtime đạt khi master healthy, worker healthy và Master UI hiển thị một worker `ALIVE` với đúng tài nguyên đã cấu hình. Sau đó mới thêm một smoke-test Spark application; chỉ khi smoke test đạt mới viết Kafka-to-Bronze/DLQ.

## 6. Runtime checkpoint

Ngày 2026-09-24, người dùng đã xác nhận:

- `spark-master` và `spark-worker` đều `healthy`.
- Master URL là `spark://spark-master:7077`.
- Master UI hiển thị đúng một worker `ALIVE`.
- Worker cung cấp đúng 2 cores và 2.0 GiB memory.
- Chưa có application nên `0 Used` và `Applications: 0` là trạng thái đúng.

Cổng Spark runtime đã đạt.

## 7. Smoke-test checkpoint

Ngày 2026-09-23, smoke test distributed compute đã đạt:

```text
SPARK_SMOKE_TEST_OK application_id=app-20260923172156-0000 row_count=100000 id_sum=5000050000 input_partitions=4
```

Test đã chứng minh driver đăng ký application với master, master cấp executor trên worker, worker xử lý bốn partition và trả kết quả tổng hợp chính xác. Test dùng client deploy mode; driver chạy trong container nhận lệnh `spark-submit`, executor chạy trên worker.

File test tạm thời và volume mount tương ứng đã được xóa sau khi xác minh. Chỉ giữ checkpoint này làm bằng chứng kiểm thử; pipeline thật sẽ có container driver và source mount riêng.

## 8. Spark 4.0.4 migration checkpoint

Ngày 2026-09-25, người dùng đã xác nhận:

- `spark-master` và `spark-worker` dùng image `spark:4.0.4-scala2.13-java17-python3-ubuntu` và đều `healthy`;
- runtime bên trong container báo Spark 4.0.4, Scala 2.13.16 và Java 17.0.20.

Cổng runtime nền tảng trên 4.0.4 đã đạt. Kết quả này chưa chứng minh Kafka connector, S3A hoặc các pipeline cũ tương thích; các integration gate phải được chạy lại độc lập.
