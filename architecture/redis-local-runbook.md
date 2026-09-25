# Redis local runtime gate

Ngày: 2026-09-24.

Mục tiêu của gate này chỉ là kiểm chứng Redis container, authentication, cấu hình memory/AOF và persistence của named volume. Chưa kết nối Spark và chưa tạo metric thật.

## 1. Cấu hình đã triển khai

- Docker Official Image `redis:8.10.2-alpine`.
- Compose profile `serving`.
- Chỉ publish `127.0.0.1:6379`; container khác dùng `redis:6379`.
- Authentication bằng `REDIS_PASSWORD`.
- Memory limit logic 256 MiB và policy `noeviction`.
- AOF `everysec` trên named volume `realtime-ecommerce-redis-data`.

`noeviction` được chọn để khi đầy bộ nhớ, write fail rõ ràng thay vì âm thầm xóa metric hoặc batch idempotency marker.

## 2. Khởi động và health gate

Người dùng chạy:

```powershell
docker compose --profile serving up -d redis
docker compose --profile serving ps redis
```

Kỳ vọng container `realtime-ecommerce-redis` ở trạng thái `healthy`.

Trước hết xác nhận kết nối không có password bị từ chối:

```powershell
docker compose --profile serving exec redis redis-cli ping
```

Kỳ vọng có lỗi `NOAUTH Authentication required`. Sau đó kiểm tra bằng credential của container:

```powershell
docker compose --profile serving exec redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ping'
```

Kỳ vọng:

```text
PONG
```

## 3. Kiểm tra cấu hình hiệu lực

```powershell
docker compose --profile serving exec redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli CONFIG GET maxmemory maxmemory-policy appendonly appendfsync'
```

Kỳ vọng tương đương:

```text
maxmemory
268435456
maxmemory-policy
noeviction
appendonly
yes
appendfsync
everysec
```

## 4. Persistence gate

Ghi duy nhất một key test có namespace rõ ràng:

```powershell
docker compose --profile serving exec redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli SET ecom:rt:v1:_infra_test persistence-check'
```

Chờ ít nhất hai giây để AOF `everysec` flush, sau đó tạo lại riêng Redis container:

```powershell
docker compose --profile serving up -d --force-recreate redis
docker compose --profile serving ps redis
docker compose --profile serving exec redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli GET ecom:rt:v1:_infra_test'
```

Kỳ vọng `persistence-check`. Sau khi đạt, xóa đúng key test:

```powershell
docker compose --profile serving exec redis sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli DEL ecom:rt:v1:_infra_test'
```

Không dùng `docker compose down -v` vì lệnh đó xóa named volumes của project.

## 5. Điều kiện hoàn thành

- Container healthy.
- Lệnh không có password bị từ chối; lệnh có password trả `PONG`.
- `maxmemory`, `noeviction`, AOF và `everysec` đúng thiết kế.
- Key test còn tồn tại sau container recreation và được cleanup sau test.

Khi gate này đạt mới triển khai Spark → Redis connectivity smoke test.

## 6. Runtime result — Redis infrastructure đạt

Người dùng đã xác nhận ngày 2026-09-24:

- container `healthy` sau recreation;
- unauthenticated `PING` trả `NOAUTH` và authenticated `PING` trả `PONG`;
- `maxmemory=268435456`, `maxmemory-policy=noeviction`;
- `appendonly=yes`, `appendfsync=everysec`;
- key `_infra_test` còn nguyên sau `--force-recreate` và cleanup trả `1`.

Redis container/auth/config/AOF/named-volume gate hoàn thành.

## 7. Spark → Redis connectivity smoke test

Test kế tiếp chạy một task trên Spark executor. Task kết nối hostname nội bộ `redis:6379`, AUTH, PING, SET một key UUID có TTL 60 giây, GET đối chiếu và DEL cleanup. Test dùng Redis RESP qua Python standard library nên chưa thêm client dependency hoặc streaming sink.

Khởi động đủ hai profile để Spark và Redis cùng có mặt:

```powershell
docker compose --profile streaming --profile serving up -d --wait redis spark-master spark-worker
```

Chạy smoke test:

```powershell
docker compose --profile streaming --profile serving exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/checks/smoke/redis_connectivity_smoke_test.py
```

Kỳ vọng:

```text
SPARK_REDIS_CONNECTIVITY_OK application_id=... endpoint=redis:6379 executor_tasks=1 cleanup=true
```

Gate này chỉ chứng minh executor DNS/network, authentication và Redis round-trip. Nó chưa chứng minh aggregate, Lua idempotency, checkpoint hoặc failure isolation của streaming job.

## 8. Runtime result — Spark → Redis connectivity đạt

Application `app-20260924152156-0000` chạy một task trên Spark executor và trả:

```text
SPARK_REDIS_CONNECTIVITY_OK endpoint=redis:6379 executor_tasks=1 cleanup=true
```

Internal DNS/network, authentication và SET/GET/DEL từ Spark worker đã được kiểm chứng. Không còn key smoke test.

## 9. Lua idempotency gate

Test áp dụng cùng một logical batch hai lần trong Redis. Lua script atomically kiểm tra batch marker, tăng counter và tạo marker. Lần đầu phải được áp dụng; lần replay phải bị bỏ qua và counter chỉ bằng một lần tăng.

```powershell
docker compose --profile streaming --profile serving exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/checks/smoke/redis_lua_idempotency_test.py
```

Kỳ vọng:

```text
REDIS_LUA_IDEMPOTENCY_OK application_id=... first_apply=1 replay_apply=0 counter=7 cleanup=true
```

Test dùng key UUID, TTL 60 giây và DEL sau khi đối chiếu. Đây là gate cho retry logic, chưa phải streaming aggregation end-to-end.

## 10. Runtime result — Lua idempotency đạt

Application `app-20260924152421-0001` trả:

```text
REDIS_LUA_IDEMPOTENCY_OK first_apply=1 replay_apply=0 counter=7 cleanup=True
```

Cùng batch được gửi hai lần nhưng counter chỉ tăng một lần; Lua check-marker/update-marker và counter update chạy atomically. Test key đã được cleanup.

## 11. Redis metric contract gate

Trước khi nối streaming sink, kiểm tra riêng business semantics của aggregate trên ba fixture VALID/WARNING/INVALID:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/checks/contracts/validate_redis_metrics.py
```

Expected core metrics:

```text
events_total=3
valid_count=1 warning_count=1 invalid_count=1
record_count=2 quantity_sum=3
gross_item_value=6000000.0
successful_labeled_value=5000000.0
rating_sum=7.0 rating_count=2
```

INVALID chỉ đóng góp vào event/quality counter; không được cộng vào business-like metrics. Marker đạt là `REDIS_METRICS_CONTRACT_OK`.

Lần chạy đầu cho thấy toàn bộ metric số đúng nhưng minute bucket thành `202609242230` thay vì `202609241530`. Nguyên nhân là fixture dùng Python naive datetime rồi Spark chuyển timezone thêm UTC+7. Fixture đã được sửa thành parse chuỗi bằng Spark session timezone; đây là lỗi test setup, không phải lỗi công thức aggregate hoặc Kafka timestamp production.

## 12. Runtime result — Redis metric contract đạt

Application `app-20260924153733-0003` xác nhận một minute bucket `202609241530` với đúng 3 event, 1 VALID, 1 WARNING, 1 INVALID; business metrics chỉ tính hai record VALID/WARNING và khớp toàn bộ expected value. Marker `REDIS_METRICS_CONTRACT_OK` đã đạt.

## 13. Kafka → Redis core streaming gate

Job v1 đọc Kafka bằng query/checkpoint riêng, chạy cùng validator, aggregate core metric theo ingestion minute và dùng Lua batch marker để update atomically:

```powershell
docker compose --profile streaming --profile serving exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.apache.spark:spark-hadoop-cloud_2.13:4.0.4 /opt/spark-apps/jobs/streaming/kafka_to_redis.py
```

Giữ producer dừng. Lần đầu query dùng `startingOffsets=earliest` để materialize snapshot Kafka còn retention. Dừng sau khi thấy batch commit và source không còn offset mới:

```text
KAFKA_REDIS_STREAMING_STARTED ...
REDIS_BATCH_COMMITTED batch_id=0 input_rows=334 ... applied=1
```

Sau đó `Ctrl+C`. `BATCH_COMMITTED` là completion signal; `STARTED` chưa đủ. Spark Kafka source có thể không gọi `foreachBatch` khi không có offset mới, nên không yêu cầu marker empty. Core keys được tạo là `summary`, `minute:*`, `latest`, `health` và `batch:*` trong namespace `ecom:rt:v1`.

Gate kế tiếp sẽ đọc Redis và đối soát quality totals `334/61/225/48/286` với baseline Bronze/DLQ trước khi kiểm thử restart.

## 14. Initial materialization và restart observation

Application `app-20260924154533-0004` commit batch 0 với 334 input thành 7 minute bucket và `applied=1`. Restart `app-20260924160417-0005` giữ nguyên query ID `fff766b6-b377-4e0b-a36a-69ebd00740ea`, đổi run ID và không tạo batch commit mới vì Kafka không có offset mới. Đây là checkpoint identity evidence; cần reconciliation để chứng minh Redis không đổi.

Sau khi dừng streaming job và giữ producer dừng, chạy read-only reconciliation:

```powershell
docker compose --profile streaming --profile serving exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/.ivy2 --executor-memory 1g --total-executor-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 /opt/spark-apps/checks/reconciliation/reconcile_redis_snapshot.py
```

Verifier tự tính expected metrics từ bounded Kafka snapshot rồi so với Redis summary, toàn bộ minute keys, latest list length/sanitization và health. Marker đạt:

```text
KAFKA_REDIS_RECONCILIATION_OK ...
```

## 15. Runtime result — Kafka/Redis reconciliation đạt

Application `app-20260924162213-0006` trả:

```text
events_total=334 valid_count=61 warning_count=225 invalid_count=48
record_count=286 minute_buckets=7 latest_length=100
```

Redis khớp bounded Kafka snapshot ở summary và từng minute key; latest list đúng giới hạn và không chứa các field bị cấm. Restart trước đó không double-count.

## 16. Cổng tiếp theo — failure isolation

Mục tiêu là chứng minh Redis outage chỉ làm query serving fail, không làm Kafka-to-Bronze/DLQ mất dữ liệu. Job Redis đã có marker `REDIS_BATCH_FAILED` trước khi rethrow lỗi để failure quan sát được.

Test cần ba terminal và input mới có kiểm soát:

1. Terminal A chạy Kafka-to-Bronze/DLQ từ checkpoint hiện có.
2. Terminal B chạy Kafka-to-Redis từ checkpoint hiện có.
3. Dừng riêng Redis, sau đó terminal C chạy producer trong khoảng ngắn rồi `Ctrl+C`.
4. Redis query phải fail rõ ràng; Bronze/DLQ query phải commit delta.
5. Dừng Bronze/DLQ và chạy physical reconciliation trong khi Redis vẫn dừng.
6. Khởi động Redis, resume Redis query và chạy Kafka/Redis reconciliation để chứng minh catch-up.

Không xóa checkpoint, key namespace hoặc named volume trong test. Kafka là buffer giữa source và hai consumer độc lập.

## 17. Runtime result — failure isolation đạt, Redis đã catch-up

Khi Redis bị dừng, serving application `app-20260924162804-0008` báo:

```text
REDIS_BATCH_FAILED batch_id=1 input_rows=4 error_type=gaierror
```

`gaierror` xuất hiện vì hostname `redis` không còn được Docker DNS phân giải sau khi container bị dừng. Query fail và không commit checkpoint batch đó.

Trong cùng sự cố, Bronze/DLQ tiếp tục xử lý input. Reconciliation `app-20260924163235-0009` đạt:

```text
kafka_count=356 bronze_count=306 dlq_count=50 sink_count=356
distinct_lineage=356 missing=0 extra=0 overlap=0 payload_mismatches=0
```

So với baseline 334/286/48, Kafka tăng 22 event, Bronze tăng 20 và DLQ tăng 2. Physical status mới là 67 VALID, 239 WARNING và 50 INVALID. Điều này chứng minh Redis outage không làm durable ingestion mất, thừa hoặc sửa payload.

Sau khi Redis hoạt động lại, serving application `app-20260924163615-0010` dùng checkpoint riêng của Redis và commit lại delta thành hai micro-batch: batch 1 có 4 record và batch 2 có 18 record, tổng cộng 22. Cách chia batch của query Redis độc lập với query Bronze/DLQ; batch ID và ranh giới micro-batch không được dùng để đối chiếu trực tiếp giữa hai consumer.

Lần reconciliation đầu ở baseline 356 thất bại duy nhất tại `health.last_input_rows`: verifier cũ so giá trị 18 của batch cuối với tổng tích lũy 356. Đây là lỗi semantic của test, không phải bằng chứng Redis thiếu dữ liệu. Verifier đã được sửa để summary/minute/latest đối soát với toàn bộ Kafka snapshot, còn health chỉ kiểm tra trạng thái `OK`, batch metadata hợp lệ và idempotency marker của batch cuối tồn tại.

Rerun application `app-20260924170510-0014` đã đóng cổng recovery:

```text
KAFKA_REDIS_RECONCILIATION_OK events_total=356 valid_count=67
warning_count=239 invalid_count=50 record_count=306 minute_buckets=8
latest_length=100 last_batch_id=2 last_input_rows=18
```

Redis cumulative state khớp toàn bộ bounded Kafka snapshot sau outage. Health trỏ đúng batch cuối `2`, batch đó có 18 input và marker idempotency còn tồn tại.

Quan sát đồng thời ở durable query cho thấy end offsets tăng từ tổng 334 lên 356, tức 22 Kafka record, trong khi marker Spark `numInputRows` hiển thị 18. Vì log không có durable batch 4 record, không được kết luận có marker bị thiếu. Marker đã được nâng cấp để in riêng `spark_input_rows` và `kafka_offset_delta`; completeness được quyết định bằng Kafka offsets cộng physical reconciliation.
