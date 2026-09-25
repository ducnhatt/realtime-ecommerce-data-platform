# Project implementation status

Cập nhật lần cuối: 2026-09-25.

Đây là trang theo dõi tiến độ hiện hành (single source of truth). `architecture.md` mô tả thiết kế và roadmap dài hạn; trang này trả lời project **đang chạy được đến đâu**, bằng chứng nào đã có và cổng tiếp theo là gì.

> **Runtime reset 2026-09-25:** toàn bộ Docker containers, images và volumes đã được người dùng chủ động xóa để giải phóng dung lượng. Các application ID/count bên dưới là bằng chứng lịch sử của các gate đã chạy thành công; môi trường runtime hiện tại trống và phải bootstrap lại trước lần kiểm thử tiếp theo.

> **Fresh bootstrap 2026-09-25:** Kafka/Spark/Redis images và MinIO community image pin digest đã được tải lại. MinIO fresh volume, Spark master và Spark worker đã được người dùng xác nhận `healthy`; `minio-init` kết thúc exit code 0. Dữ liệu, Kafka topic và các checkpoint cũ không còn vì Docker volumes đã bị xóa.

> **Spark version migration 2026-09-25 — 🟡 runtime nền tảng đạt, integration regression đang chờ:** Spark image, `.env`, package Kafka/S3A và các runbook hiện hành đã được chuẩn hóa từ 4.2.0 sang **4.0.4/Scala 2.13/Java 17** để phù hợp nhánh được ClickHouse Spark connector hỗ trợ. Người dùng đã xác nhận master/worker đều `healthy`; `spark-submit --version` trả Spark 4.0.4, Scala 2.13.16 và Java 17.0.20. Kafka vẫn là 4.2.0. Toàn bộ application ID và số liệu pipeline bên dưới là bằng chứng lịch sử trên Spark 4.2.0; Kafka connector, S3A, Bronze/DLQ, Silver và Redis phải qua regression mới trên 4.0.4 trước khi được công nhận lại.

> **S3A regression attempt 2026-09-25 — chưa đạt do runtime network:** artifact `spark-hadoop-cloud_2.13:4.0.4` đã nạp đủ để khởi tạo S3A và thực hiện request, nhưng driver không phân giải được Docker hostname `minio` (`java.net.UnknownHostException: minio`). Chưa có bằng chứng về lỗi binary compatibility. Cần khôi phục/kiểm tra MinIO trên `ecommerce-net`, xác minh DNS từ `spark-master`, rồi mới rerun smoke test. Cleanup của test đã được đổi sang best-effort để không che lỗi gốc khi storage unavailable.

> **Network recovery:** `ecommerce-net` đã hiện đủ MinIO, Spark master, Spark worker và Kafka. Từ `spark-master`, hostname `minio` phân giải thành `172.18.0.5` và endpoint `/minio/health/live` trả HTTP 200. Điều này loại trừ lỗi khởi động/API của image MinIO tại thời điểm kiểm tra; S3A smoke test vẫn phải chạy lại để xác nhận đường ghi/đọc qua executor.

> **Spark 4.0.4 → MinIO/S3A regression — ✅ đạt:** application `app-20260925082819-0001` ghi hai Parquet file, đọc lại đúng 100 row với tổng ID 5050 và xóa sạch path test (`cleanup=true`). Gate xác nhận distributed execution, `spark-hadoop-cloud_2.13:4.0.4`, Docker DNS, MinIO API/credentials và Parquet write/read/delete hoạt động cùng nhau. Cảnh báo SLF4J NOP không ảnh hưởng kết quả; bước kế tiếp là Kafka connector 4.0.4.

> **Kafka fresh bootstrap sau runtime reset — ✅ đạt:** `create_data.py --mode realtime` đã tạo lại topic `ecommerce_reviews` với 10 partition/retention một ngày và phát sample event qua host listener. `kafka-topics.sh --bootstrap-server kafka:29092 --list` nhìn thấy topic từ Docker network. Số event và khả năng đọc bằng Spark chưa được suy đoán từ log producer; chờ bounded Kafka validation trên connector 4.0.4.

> **Spark 4.0.4 → Kafka/validator regression — ✅ đạt:** application `app-20260925083222-0002` đọc bounded snapshot 27 event qua `kafka:29092` bằng `spark-sql-kafka-0-10_2.13:4.0.4`, tạo đúng 27 output và 27 lineage duy nhất. Phân loại gồm 2 VALID, 18 WARNING và 7 INVALID; tổng status bằng input/output. Dữ liệu xuất hiện trên 8/10 partition là hợp lệ với mẫu nhỏ và message key null, không phải mất partition hay mất record.

> **Spark 4.0.4 Bronze/DLQ initial materialization — chờ reconciliation:** application `app-20260925083422-0003` khởi động hai query mới trên checkpoint fresh. Batch 0 của cả hai báo 27 input; batch 1 báo 0, cho thấy không còn input mới tại thời điểm quan sát. Tuy nhiên Spark progress trả end offset rỗng nên marker cũ in sai `kafka_offset_delta=0`; code đã đổi sang `UNAVAILABLE` khi thiếu offset telemetry. Chưa công nhận sink correctness cho đến khi bounded Kafka/Bronze/DLQ physical reconciliation đạt.

## 1. Trạng thái tổng quan

**Vị trí hiện tại:** durable Kafka → Bronze/DLQ, Redis recovery, Silver full-rebuild và Silver incremental `availableNow` đã được kiểm chứng trước runtime reset. Warehouse direction đã chuyển sang ClickHouse-first để ưu tiên dữ liệu tăng lớn; bước kế tiếp là ClickHouse local infrastructure gate.

**Spark source organization:** entrypoint đã được tách thành `jobs/streaming`, `jobs/incremental`, `jobs/recovery` và `checks/{reconciliation,contracts,smoke}`; transformation dùng lại nằm trong `pipeline`. Không xóa verification artifact, nhưng chúng không còn bị trình bày như production jobs. Toàn bộ runbook đã cập nhật sang path mới; xem [Spark layout](../spark/README.md).

```text
create_data.py ──✅──> Kafka ──✅ streaming──> Spark validator ──✅──> Bronze
                       ✅                       │ checkpoint ✅          MinIO ✅
                  persistent log               └───────────────✅──> DLQ
```

Điểm quan trọng: streaming application đã đọc Kafka, validate, phân tuyến, ghi Bronze/DLQ và phục hồi bằng checkpoint. Silver v1 chạy bounded full-rebuild riêng: preflight Bronze, transform, Snappy Parquet overwrite theo schema-version/ingest-date và read-back validation. Phạm vi lịch sử đã đạt ingestion, Redis serving và Silver local; chưa có ClickHouse/Gold và chưa tuyên bố exactly-once end-to-end hay HA.

### Quy ước

| Ký hiệu | Ý nghĩa |
|---|---|
| ✅ | Đã được người dùng kiểm chứng runtime |
| 🟡 | Đã thiết kế hoặc mới đạt một phần, chưa qua đủ cổng runtime |
| ⬜ | Chưa triển khai |
| ⚠️ | Giới hạn được chấp nhận có chủ đích |

## 2. Các cổng đã đạt

| Cổng | Trạng thái | Bằng chứng |
|---|---|---|
| Source contract 32 trường | ✅ runtime | Contract, JSON Schema và sáu case đã được validator Spark kiểm chứng |
| Kafka KRaft local | ✅ runtime | Broker healthy; topic `ecommerce_reviews`: 10 partition, RF=1, retention 1 ngày |
| Producer → Kafka | ✅ runtime | Producer phát event và console consumer đọc được JSON |
| Kafka persistence | ✅ runtime | Sau `down → up`, consumer đọc lại message cũ từ named volume |
| MinIO server và bucket | ✅ runtime | Server healthy; init tạo `bronze`, `silver`, `dlq` |
| MinIO persistence | ✅ runtime | Tạo lại container không làm mất ba bucket |
| Spark Standalone | ✅ runtime | Một master, một worker `ALIVE`, 2 cores/2 GiB |
| Spark distributed compute | ✅ runtime | App `app-20260923172156-0000`: 4 partition, count 100.000, sum 5.000.050.000 |
| Spark 4.0.4 → Kafka + validator | ✅ runtime | App `app-20260925083222-0002`: 27 input/output/lineage, 2 VALID + 18 WARNING + 7 INVALID qua connector 4.0.4 |
| Runtime schema parsing | ✅ runtime | App `app-20260924045552-0000`: malformed/missing/null/type được phân biệt đúng |
| Data-quality routing | ✅ runtime | Sáu case ra đúng VALID/WARNING/INVALID và stable rule ID; marker `CONTRACT_VALIDATION_OK` |
| Kafka snapshot + validator | ✅ runtime | App `app-20260924084421-0001`: 168 input/output/lineage, đủ 10 partition; marker `KAFKA_VALIDATION_OK` |
| Spark 4.0.4 → MinIO/S3A | ✅ runtime | App `app-20260925082819-0001`: ghi/đọc 100 row trong 2 Parquet file, tổng ID 5050 và cleanup thành công |
| Bronze/DLQ reconciliation | ✅ runtime | App `app-20260924103733-0001`: Kafka 231 = Bronze 195 + DLQ 36; lineage/hash đều khớp |
| Bronze/DLQ Structured Streaming | ✅ runtime | Kafka 356 = Bronze 306 + DLQ 50; lineage và payload reconciliation đều đạt |
| Checkpoint recovery/no replay | ✅ runtime | Backlog 29 được catch-up; restart không input giữ nguyên 334 row và 12 file mỗi sink |

## 3. Hạng mục chưa hoàn thành

| Hạng mục | Trạng thái | Phần còn thiếu |
|---|---|---|
| Producer hardening | 🟡 | Message key đang null; chưa chốt callback/retry/graceful shutdown và vai trò JSONL replay |
| Silver full-rebuild | ✅ runtime | Công cụ bootstrap/recovery: `306 → 306`, lineage/hash/schema đúng, 2 file và rerun không append trùng |
| Silver incremental | ✅ runtime | Initial 306/306, no-input/no-replay và controlled delta +20 đều đạt; Bronze = Silver = 326 |
| Realtime serving/Redis | ✅ runtime | Outage không ảnh hưởng durable branch; Redis catch-up 22 event và reconcile đủ 356 event |
| ClickHouse/dbt Gold | 🟡 design | Đã chọn ClickHouse-first và chốt design v1; chưa dựng warehouse runtime |
| Airflow | ⬜ | Chưa có nhu cầu orchestration đã kiểm chứng |
| Observability/dashboard | ⬜ | Chưa có metric/alert/dashboard end-to-end |
| Power BI/E2E | ⬜ | Chưa có data mart và kiểm thử toàn luồng |

## 4. Tư duy hệ thống tại checkpoint hiện tại

### Ranh giới và giao diện

- Windows producer giao tiếp Kafka qua external listener `localhost:9092`.
- Container Spark giao tiếp Kafka qua internal listener `kafka:29092`.
- Spark master là control plane; Spark worker là compute/data plane.
- MinIO cung cấp S3 API nội bộ tại `minio:9000`; Spark đã kiểm chứng write/read/delete Parquet qua S3A.

Việc kiểm thử từng ranh giới độc lập làm giảm phạm vi chẩn đoán: lỗi Spark → Kafka không bị nhầm với lỗi S3A hoặc validation.

### Nơi giữ state

| State | Chủ sở hữu hiện tại | Độ bền |
|---|---|---|
| Event và offset Kafka | Kafka named volume | Đã kiểm tra restart; chỉ giữ một ngày |
| Bucket/object | MinIO named volume | Bronze 306 row, DLQ 50 row; 14 Parquet file mỗi sink tại baseline hiện tại |
| Spark application state | MinIO `checkpoints` bucket | Hai query có checkpoint độc lập và đã kiểm chứng restart |
| Streaming checkpoint | MinIO S3A | Query ID ổn định, run ID thay đổi; backlog catch-up và no-replay đã đạt |
| Business deduplication state | Chưa có | Nguồn không có eventID; chỉ có Kafka topic/partition/offset để truy vết |

### Failure domain hiện tại

- Kafka, MinIO, Spark master và worker đều single-node. Mỗi service là single point of failure.
- Kafka RF=1 và MinIO single-node phù hợp local learning, không đại diện HA production.
- Nếu Spark dừng, Kafka có thể làm buffer nhưng chỉ trong retention một ngày; consumer nghỉ lâu hơn có nguy cơ mất input.
- Nếu MinIO dừng trong pipeline tương lai, sink phải fail rõ ràng và không commit offset/checkpoint sai.
- Checkpoint recovery đã được chứng minh trong controlled restart; chưa kiểm thử crash giữa commit, checkpoint corruption hoặc hai driver đồng thời.

### Capacity và scale

- Kafka có 10 partition nhưng Spark worker chỉ có 2 core; tối đa khoảng hai task chạy đồng thời, các partition còn lại được xử lý theo lượt.
- Cấu hình này cố ý tạo cơ hội quan sát parallelism nhưng không tối ưu throughput cho laptop.
- Tăng partition không tự làm pipeline nhanh hơn nếu không tăng executor core; ngược lại còn tăng scheduling overhead và small-file risk.
- Tốc độ producer hiện thấp nên bottleneck chưa phải throughput. Ưu tiên correctness, recovery và observability trước tuning.

### Rủi ro/tech debt đã biết

1. MinIO Community là legacy và chỉ được phép dùng local.
2. Kafka message key null nên chưa có ordering theo business entity.
3. Kafka connector đang được resolve bằng `--packages`; production-like setup cần image/dependency pin sẵn.
4. Credentials MinIO là development default, không được dùng ngoài local.
5. Source có review time nhưng không có order event time; KPI thời gian bị giới hạn.
6. Store/product/status là dữ liệu giả lập không ổn định; không được diễn giải quá mức.
7. Cloud dependency phát cảnh báo thiếu SLF4J `StaticLoggerBinder`; không ảnh hưởng smoke I/O nhưng cần xử lý/đóng gói classpath rõ ràng trước môi trường production-like.

### Rule taxonomy đã chốt

- JSON syntax và khả năng parse là technical validation.
- Presence/type/null là schema/data-contract validation.
- Enum/range là domain constraint theo cam kết của generator.
- Quan hệ giữa nhiều trường là cross-field consistency; chỉ chặn khi contract cam kết chắc chắn.
- Hiện tượng chỉ đáng nghi về nghiệp vụ được gắn `WARNING`, không tự động loại record.

Quyết định này ngăn hai lỗi thiết kế đối lập: cho dữ liệu vi phạm contract đi tiếp, hoặc loại dữ liệu hợp lệ chỉ vì một giả định business chưa được xác nhận.

## 5. Checkpoint Kafka + validation đã đạt

Application `app-20260924084421-0001` đọc snapshot thật từ `ecommerce_reviews` và trả marker `KAFKA_VALIDATION_OK`:

- `message_count = output_count = distinct_lineage = 168`;
- dữ liệu phủ đủ 10 Kafka partition;
- 27 VALID, 116 WARNING và 25 INVALID;
- toàn bộ 25 INVALID chỉ có `E006_REQUIRED_NULL`, phù hợp chaos injection khoảng 15%;
- 110 lần W101 và 31 lần W102; một record có thể mang cả hai warning nên không cộng hai rule count để suy ra số WARNING;
- không xuất hiện malformed, missing/unexpected field, type mismatch hoặc cross-field error trên snapshot này.

Tỷ lệ của snapshot: VALID 16,07%, WARNING 69,05%, INVALID 14,88%. Đây là quality profile của một mẫu random, không phải SLA. W101 cao phản ánh generator chọn brand độc lập với category và mapping “tự nhiên” của validator hẹp; nó không phải lỗi kỹ thuật. Không có E206 cho thấy cấu hình timezone nguồn đang nhất quán trên snapshot.

## 6. Cổng tiếp theo

**Spark ↔ MinIO S3A connectivity — ✅ đạt.**

Application `app-20260924090739-0000` ghi 100 row thành 2 Parquet file qua S3A, đọc lại đúng `row_count=100`, `id_sum=5050`, sau đó xóa path UUID và xác nhận `cleanup=true`. Bucket `bronze` không giữ lại object test. Dependency, internal DNS/network, authentication, bucket permission và Parquet I/O đều đã qua gate.

**Bronze/DLQ Structured Streaming initial run — ✅ materialization đã xác nhận.**

MinIO UI và `mc` read-only xác nhận cả Bronze/DLQ đều có Parquet, `_spark_metadata` và checkpoint riêng. Hai query cùng có batch `0..4`; checkpoint batch 4 đã commit và có cùng end offsets trên 10 partition. Initial offsets đều bằng 0, end offsets cộng lại bằng 231, nên cả hai query đã tiến qua cùng 231 Kafka record tại checkpoint quan sát.

Bronze có 6 Parquet file: một file cho `2026-09-23` và năm file cho `2026-09-24`. DLQ cũng có cấu trúc 1 + 5 file tương ứng. Batch đầu chứa hai date partition nên dù `coalesce(1)`, writer vẫn tạo một file cho mỗi partition value; đây là behavior đúng và là đầu vào cho quyết định compaction sau này.

`bronze/_connectivity_test/` còn một directory marker 0 byte. Dữ liệu UUID của smoke test đã bị xóa, nhưng không được nói bucket hoàn toàn không còn object test. Marker vô hại và chưa bị tự ý xóa.

Xem [Bronze/DLQ streaming runbook](bronze-dlq-streaming-runbook.md).

**Physical reconciliation — ✅ đạt.**

Application `app-20260924103733-0001` đối soát snapshot ổn định thành công:

- Kafka 231 record = Bronze 195 + DLQ 36;
- 35 VALID + 160 WARNING = 195 Bronze;
- 36 INVALID = 36 DLQ;
- 231 lineage duy nhất, `missing=0`, `extra=0`, `overlap=0`;
- `payload_mismatches=0`, nên raw payload không đổi từ Kafka tới object storage;
- 6 Bronze Parquet file và 6 DLQ Parquet file đúng với inventory đã đọc.

Tỷ lệ snapshot: VALID 15,15%, WARNING 69,26%, INVALID 15,58%. Đây là quality profile của dữ liệu random, không phải SLA.

**Checkpoint restart/resume — ✅ đạt.**

Baseline ban đầu là 231 record và 6 file mỗi sink. Restart application `app-20260924110503-0002` sau đó reconciliation bằng `app-20260924112115-0003` cho thấy Kafka/sink tăng lên 305 record, 11 file mỗi sink. Delta 74 record được phân tuyến thành +65 Bronze và +9 DLQ; toàn bộ 305 lineage vẫn duy nhất với missing/extra/overlap/payload mismatch đều bằng 0.

Lần reconciliation tiếp theo `app-20260924112720-0004` thấy Kafka đã tăng tiếp lên 334 trong khi sinks vẫn là 305, nên thất bại có chủ đích với `missing=29`. Kết quả này không chỉ ra duplicate hay mất dữ liệu khỏi Kafka; nó cho biết producer/source còn tạo thêm 29 event sau khi stream dừng và backlog chưa được sink xử lý. Baseline 305 vì thế không còn là snapshot source ổn định.

Restart `app-20260924113040-0005` giữ nguyên Bronze/DLQ query ID và đổi run ID, xác nhận hai logical query đã khôi phục đúng checkpoint. Nhưng reconciliation `app-20260924113120-0006` vẫn thấy 334/305 và thiếu 29 lineage: application đã start nhưng chưa có micro-batch catch-up được commit trước khi dừng. Không dùng riêng marker STARTED làm tín hiệu hoàn thành xử lý.

Đã bổ sung observability marker `BRONZE_DLQ_BATCH_COMMITTED` cho từng query, gồm batch ID, input row count và Kafka end offsets. Lần chạy tiếp theo phải chờ marker của cả Bronze và DLQ trước khi dừng và reconciliation; đây là completion signal thay cho thời gian chờ ước lượng.

Catch-up application `app-20260924114510-0007` đã commit batch 10 với đúng 29 input row trên cả hai query; batch 11 tiếp theo có 0 input và giữ nguyên end offsets tổng 334, xác nhận stream đã bắt kịp Kafka. Reconciliation `app-20260924114655-0008` đạt: Kafka 334 = Bronze 286 + DLQ 48, gồm 61 VALID + 225 WARNING + 48 INVALID; 334 lineage duy nhất, `missing=extra=overlap=payload_mismatches=0`, mỗi sink có 12 Parquet file.

Baseline ổn định mới cho no-input control là 334 record, Bronze 286, DLQ 48 và 12 file mỗi sink.

No-input restart sau đó được reconciliation `app-20260924115406-0010` xác nhận giữ nguyên toàn bộ baseline: Kafka 334, Bronze 286, DLQ 48, 12 file mỗi sink và mọi chỉ số missing/extra/overlap/payload mismatch bằng 0. Query ID giữ nguyên/run ID thay đổi đã được xác nhận. Xem [Checkpoint restart/resume test](checkpoint-restart-resume-test.md).

Runtime gate đã khép kín: initial materialization ✅ → physical reconciliation ✅ → delta catch-up ✅ → no-input restart/no replay ✅.

Các quyết định đã chốt cho bước streaming sau gate này:

1. dùng `spark-hadoop-cloud_2.13:4.0.4` đồng bộ với Spark runtime, không trộn Hadoop/AWS SDK tùy ý;
2. Parquet schema và layout object cho Bronze/DLQ;
3. partition strategy để tránh small files;
4. checkpoint location và quyền sở hữu state;
5. idempotency khi retry một micro-batch;
6. failure semantics nếu Bronze thành công nhưng DLQ thất bại, hoặc ngược lại;
7. dùng hai query/checkpoint riêng cho Bronze và DLQ, rồi kiểm thử restart/reconciliation.

Chưa được tuyên bố exactly-once chỉ dựa trên Spark checkpoint. Hai physical sink không tự có một transaction chung.

**Redis runtime foundation — ✅ đạt:** container/auth/config/AOF/named-volume persistence đã được người dùng kiểm chứng, gồm key tồn tại sau container recreation và cleanup thành công.

**Spark → Redis connectivity — ✅ đạt:** application `app-20260924152156-0000` chạy trên executor, AUTH/PING/SET/GET/DEL qua `redis:6379` và cleanup thành công.

**Lua batch idempotency — ✅ đạt:** application `app-20260924152421-0001` trả first apply 1, replay 0, counter 7 và cleanup true; retry cùng batch không double-count.

**Cổng tiếp theo:** Redis metric contract regression trên fixtures. Gate tách correctness của business-like aggregate khỏi network, Redis mutation và streaming checkpoint trước khi ghép end-to-end. Xem [Redis local runtime gate](redis-local-runbook.md).

Lần chạy đầu: mọi metric số đều đúng, nhưng fixture timestamp bị cộng UTC+7 (`22:30` thay vì `15:30`) do Python naive datetime. Test setup đã sửa sang Spark session-timezone parsing; gate vẫn chờ rerun thành công.

Rerun `app-20260924153733-0003` đạt `REDIS_METRICS_CONTRACT_OK`: minute bucket `202609241530` và toàn bộ 10 metric khớp expected.

**Kafka → Redis initial materialization — ✅ đạt:** application `app-20260924154533-0004` commit 334 input thành 7 minute bucket với `applied=1`. Restart `app-20260924160417-0005` giữ query ID và đổi run ID; không có offset mới nên không có callback/batch commit mới.

**Cổng tiếp theo:** dừng streaming và chạy read-only Kafka/Redis reconciliation. Verifier tính expected từ bounded Kafka snapshot rồi so summary, minute keys, latest sanitization và health; kết quả này mới đóng no-replay gate.

**Kafka ↔ Redis reconciliation/no replay — ✅ đạt:** application `app-20260924162213-0006` xác nhận Redis khớp Kafka ở 334 event, 61 VALID, 225 WARNING, 48 INVALID, 286 business-eligible record, 7 minute bucket và latest list 100 record sanitized.

**Cổng tiếp theo:** Redis failure isolation. Cố ý dừng riêng Redis khi cả durable và serving consumer đang chạy, phát delta mới, yêu cầu Redis query fail rõ ràng nhưng Bronze/DLQ vẫn commit và reconcile; sau đó Redis resume/catch-up.

**Redis failure isolation — ✅ durable branch đạt:** Redis application `app-20260924162804-0008` fail ở batch 1 với 4 input do DNS/Redis unavailable. Bronze/DLQ vẫn xử lý toàn bộ 22 event mới; reconciliation `app-20260924163235-0009` đạt Kafka 356 = Bronze 306 + DLQ 50, không missing/extra/overlap/payload mismatch.

**Redis recovery — ✅ đạt:** application `app-20260924163615-0010` commit batch 1 = 4 và batch 2 = 18, cộng đủ delta 22 theo checkpoint riêng của serving query. Reconciliation đầu chỉ fail vì test cũ hiểu sai `health.last_input_rows=18` (số input của batch cuối) thành cumulative total 356; summary/minute/latest không báo mismatch. Sau khi sửa contract, application `app-20260924170510-0014` đạt: 356 total, 67 VALID, 239 WARNING, 50 INVALID, 306 business-eligible record, 8 minute bucket, latest list 100, health batch 2/18 và batch marker hợp lệ.

**Durable progress observability:** log Bronze/DLQ được cung cấp chỉ có batch 12 báo `numInputRows=18`, nhưng tổng end offsets tăng từ 334 lên 356 và physical reconciliation chứng minh đủ 22 record. Không có bằng chứng về một durable batch 4 dòng bị thiếu khỏi log. Marker được sửa để tách `spark_input_rows` và `kafka_offset_delta`; khi hai metric mâu thuẫn, Kafka offsets cùng physical reconciliation là bằng chứng completeness.

## 7. Cách cập nhật tiến độ từ đây

Sau mỗi bước, cập nhật năm phần:

1. **Decision:** chọn gì và business/technical requirement nào dẫn tới lựa chọn đó.
2. **System thinking:** boundary, state, failure mode, scaling, observability và security.
3. **Trade-off:** điều gì đạt được và điều gì chủ động chưa giải quyết.
4. **Runtime evidence:** command/output/application ID hoặc dữ liệu nhìn thấy trong storage.
5. **Next gate:** điều kiện cụ thể phải đạt trước khi chuyển bước.

Chỉ đánh dấu ✅ khi có bằng chứng runtime do người dùng xác nhận. Thiết kế hoặc file cấu hình chưa chạy chỉ được đánh dấu 🟡.

## 8. Cách trình bày trung thực trong CV/phỏng vấn ở thời điểm này

Có thể nói:

- Thiết kế môi trường local gồm Kafka KRaft, MinIO S3-compatible và Spark Standalone bằng Docker Compose.
- Cấu hình dual Kafka listener cho producer host và Spark container.
- Kiểm chứng persistence của Kafka/MinIO qua container recreation.
- Kiểm chứng Spark scheduling/executor và Spark đọc Kafka trên 10 partition.
- Xây reusable native-Spark validator và regression gate cho malformed/missing/null/type/cross-field/business warning.
- Kiểm chứng validator trên 168 Kafka event thật, bảo toàn row/lineage và phủ đủ 10 partition.
- Xây Kafka-to-Bronze/DLQ Structured Streaming với hai checkpoint độc lập, lineage theo topic-partition-offset và physical reconciliation.
- Kiểm chứng restart phục hồi backlog 29 record và no-input restart không tạo row/file trùng.
- Ghi nhận rõ các giới hạn single-node, RF=1, MinIO legacy, null message key và retention một ngày.

Chưa nên nói:

- Đã xây xong realtime data platform.
- Đã có exactly-once processing.
- Đã triển khai Bronze/Silver/Gold end-to-end.
- Hệ thống có high availability hoặc production readiness.
