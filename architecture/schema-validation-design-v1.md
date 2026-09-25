# Schema parsing and validation design v1

Ngày: 2026-09-24. Trạng thái: thiết kế và batch regression đã được kiểm chứng runtime.

## 1. Mục tiêu và phạm vi

Biến Kafka `value` dạng bytes/string thành một record có kiểu, có lineage và có kết quả định tuyến xác định:

```text
Kafka value + metadata
          |
          v
  Safe JSON parsing
          |
          v
 Structural + field + cross-field validation
          |
          +---- VALID   ----> Bronze
          +---- WARNING ----> Bronze + warning codes
          +---- INVALID ----> DLQ + error codes + raw payload
```

Bước này chỉ thiết kế parsing/validation và chuẩn đầu ra logic. Chưa chạy stream liên tục, chưa ghi MinIO và chưa quyết định cơ chế multi-sink commit.

## 2. Quyết định parsing

### Lớp 1 — giữ raw và Kafka lineage

Không sửa hoặc bỏ Kafka value gốc. Mỗi record trước validation phải có:

- `raw_payload`: Kafka value cast sang string.
- `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`.
- `ingested_at`: thời điểm job nhận/xử lý record.
- `contract_version = ecommerce-event-v1`.

Khóa lineage kỹ thuật là `(kafka_topic, kafka_partition, kafka_offset)`. Đây không phải business deduplication key.

### Lớp 2 — safe JSON parse bằng VARIANT

Dùng Spark 4.0.4 `try_parse_json(raw_payload)`:

- JSON hợp lệ tạo `VariantType`.
- JSON malformed trả SQL `NULL`, không ném exception làm chết toàn bộ micro-batch.
- JSON hợp lệ nhưng top-level là array/scalar/null vẫn bị đánh dấu `INVALID`, vì contract yêu cầu object.

Dùng `schema_of_variant`/VARIANT khi cần xác minh JSON type gốc. Điều này tránh trường hợp typed parser âm thầm coercion, ví dụ chuỗi số thành số.

### Lớp 3 — kiểm tra key presence

Dùng `json_object_keys(raw_payload)` lấy key top-level:

- `missing_fields = required_fields - actual_fields`.
- `unexpected_fields = actual_fields - required_fields`.
- Field xuất hiện với JSON `null` vẫn được xem là **có mặt**, nên có thể phân biệt với field bị thiếu.

V1 giữ `additionalProperties = false`: field lạ là `INVALID`. Trade-off là source thêm field mới sẽ làm tăng DLQ; đây là tín hiệu buộc version contract thay vì âm thầm bỏ dữ liệu.

### Lớp 4 — typed struct

Dùng `from_json` với `StructType` tường minh cho đúng 32 trường. Không infer schema từ dữ liệu runtime vì inference có thể thay đổi theo sample và khiến micro-batch không ổn định.

Kiểu mục tiêu Bronze:

- ID/text/status: `STRING`.
- `helpful`: `ARRAY<INT>`.
- `overall`: `DOUBLE` ở Bronze; Silver mới chuẩn hóa integer sau validation.
- `unixReviewTime`, `day_diff`, `helpful_yes`, `total_vote`, `quantity`: `LONG/INT` phù hợp range.
- `unitPrice`, `totalAmount`: giữ numeric ở Bronze; Silver chuyển `DECIMAL`.

## 3. Vì sao không dùng Python UDF cho validation chính?

**Chọn:** Spark SQL functions/expressions (`when`, `array`, `array_compact`, regex, enum, arithmetic, VARIANT functions).

**Lý do:**

- Catalyst có thể tối ưu expression plan.
- Tránh serialize từng row JVM ↔ Python.
- Error/warning codes là column data, dễ group/count để quan sát.
- Cùng transformation dùng được cho batch test và Structured Streaming.

**Trade-off:** code expression dài và khó đọc hơn Python thuần; VARIANT khóa implementation vào Spark 4.x. Rule phải được chia thành hàm nhỏ và có stable rule ID.

Python UDF chỉ được xem xét lại nếu xuất hiện validation không biểu diễn hợp lý bằng Spark expression và benchmark chứng minh chi phí chấp nhận được.

## 4. Thứ tự validation

Validation chạy theo lớp để tránh error cascade:

1. JSON syntax/top-level.
2. Field presence và unexpected field.
3. JSON type/nullability.
4. Pattern, enum và range.
5. Cross-field consistency.
6. Business warnings.

Nếu JSON malformed, record chỉ cần lỗi gốc như `E001_MALFORMED_JSON`; không sinh thêm 32 lỗi `missing/null`. Nếu thiếu `quantity`, không tiếp tục kết luận `totalAmount` sai do phép nhân thiếu input.

### Phân loại rule theo ý nghĩa

Không gọi chung toàn bộ validation là “business rule”. V1 phân biệt:

| Lớp | Ví dụ | Kết quả |
|---|---|---|
| Technical parsing | JSON malformed, top-level không phải object | `INVALID`, DLQ |
| Schema/data contract | thiếu key, sai type, required null | `INVALID`, DLQ |
| Domain constraint | rating 1–5, quantity 1–4, enum hợp lệ | `INVALID`, DLQ |
| Cross-field consistency | tổng tiền, helpful, category/sub-category, region/city, review time | `INVALID`, DLQ |
| Business warning | brand/category bất thường, payment/shipping đáng nghi | `WARNING`, vẫn giữ để xử lý |

Một số cross-field rule có ý nghĩa nghiệp vụ, nhưng chỉ được coi là lỗi chặn khi source contract cam kết quan hệ đó. Tổ hợp chỉ “đáng nghi” không bị loại khi chưa có yêu cầu business chắc chắn; hệ thống gắn warning để quan sát và tránh mất dữ liệu.

## 5. Rule catalog v1

### Structural errors — vào DLQ

| Rule ID | Điều kiện |
|---|---|
| `E001_MALFORMED_JSON` | `try_parse_json` trả SQL null |
| `E002_TOP_LEVEL_NOT_OBJECT` | JSON hợp lệ nhưng không phải object |
| `E003_MISSING_FIELDS` | Thiếu ít nhất một trong 32 key bắt buộc |
| `E004_UNEXPECTED_FIELDS` | Có key ngoài contract v1 |
| `E005_TYPE_MISMATCH` | JSON type không đúng contract |
| `E006_REQUIRED_NULL` | Field khác `reviewerName` có JSON null |

### Field errors — vào DLQ

| Nhóm rule | Điều kiện |
|---|---|
| ID pattern | `reviewerID`, `asin`, order/payment/shipping ID sai pattern |
| Enum | category, sub-category, region, store type, payment/shipping/carrier ngoài tập cho phép |
| Range | rating, price, quantity, vote hoặc timestamp ngoài range contract |
| Text | Các text field bắt buộc rỗng |
| Array | `helpful` không có đúng hai integer hợp lệ |

Implementation sẽ gán stable ID cụ thể cho từng field, ví dụ `E101_REVIEWER_ID_PATTERN`, thay vì chỉ lưu câu tiếng Việt tự do.

### Cross-field errors — vào DLQ

| Rule ID | Điều kiện |
|---|---|
| `E201_TOTAL_AMOUNT_MISMATCH` | `totalAmount != unitPrice * quantity` |
| `E202_HELPFUL_MISMATCH` | `helpful != [helpful_yes, total_vote]` |
| `E203_HELPFUL_EXCEEDS_TOTAL` | `helpful_yes > total_vote` |
| `E204_CATEGORY_SUBCATEGORY_MISMATCH` | sub-category không thuộc category |
| `E205_REGION_CITY_MISMATCH` | city không thuộc region |
| `E206_REVIEW_TIME_MISMATCH` | `reviewTime` không cùng ngày với `unixReviewTime` |

So sánh `E206` chạy với `spark.sql.session.timeZone` lấy từ `SOURCE_TIME_ZONE`, mặc định `Asia/Ho_Chi_Minh`, để khớp cách generator gọi `datetime.fromtimestamp`. Không dựa vào timezone mặc định ngầm của container.

So sánh tiền dùng decimal/cast phù hợp hoặc tolerance được chốt trong implementation test; không dùng so sánh float thiếu kiểm soát.

### Business warnings — vẫn vào Bronze

| Rule ID | Điều kiện |
|---|---|
| `W101_UNUSUAL_BRAND_CATEGORY` | Brand/category không tự nhiên theo mapping phân tích v1 |
| `W102_PAYMENT_SHIPPING_INCONSISTENT` | Ví dụ thanh toán thất bại nhưng trạng thái đã giao/đang vận chuyển |

Hai cảnh báo này không phản ánh vi phạm contract của generator nên không được đẩy DLQ.

`storeID` không ổn định giữa các record cần state/canonical store dimension, nên hoãn khỏi stateless validator v1. Review date 2023–2024 là đặc tính cố ý của nguồn; theo dõi bằng metric source-age, không biến mọi record thành `WARNING`.

## 6. Quy tắc tính status

Tách hai mảng:

- `validation_errors: array<string>`.
- `validation_warnings: array<string>`.

```text
Nếu validation_errors không rỗng              => INVALID
Nếu errors rỗng và warnings không rỗng        => WARNING
Nếu cả hai rỗng                               => VALID
```

Không dùng một chuỗi message duy nhất vì khó aggregate và dễ thay đổi wording. Stable rule ID dùng cho metric; mô tả rule nằm trong code/tài liệu.

## 7. Logical output contract

### Bronze — chỉ VALID/WARNING

- `payload`: struct 32 trường đã parse.
- `raw_payload`: JSON gốc để audit/reprocess.
- Kafka lineage: topic/partition/offset/timestamp.
- `ingested_at`, `contract_version`.
- `validation_status`, `validation_errors` (rỗng), `validation_warnings`.

### DLQ — chỉ INVALID

- `raw_payload` bắt buộc giữ nguyên.
- Kafka lineage và `ingested_at`.
- `contract_version`.
- `validation_status = INVALID`.
- `validation_errors`, `validation_warnings` nếu có.
- Parsed payload có thể null/partial; không dùng làm nguồn replay chính.

DLQ không phải nơi xóa bỏ dữ liệu. Nó là luồng có thể audit, sửa rule/source và replay theo lineage.

## 8. Tư duy hệ thống

### State và idempotency

Validation là transformation stateless theo từng record. Store consistency bị hoãn vì cần state xuyên event. State duy nhất của streaming job sau này là Kafka offsets/checkpoint và sink commit metadata.

Bronze và DLQ là hai sink. Ghi hai sink trong một `foreachBatch` không tự tạo transaction chung: Bronze có thể thành công rồi DLQ thất bại. Thiết kế storage tiếp theo phải dùng `batch_id`/deterministic path hoặc cơ chế commit để retry không tạo duplicate. Chưa tuyên bố exactly-once trước khi test failure giữa hai write.

### Failure modes

- JSON xấu là lỗi dữ liệu và phải đi DLQ, không làm query chết.
- Lỗi code/schema/dependency là lỗi pipeline và phải làm query fail để được phát hiện; không biến mọi exception thành DLQ.
- Nếu tỷ lệ DLQ tăng đột ngột, có thể là schema evolution hoặc producer regression.
- Nếu MinIO lỗi, không được coi batch là hoàn tất chỉ vì validation đã chạy.

### Scaling

Native expressions scale theo Spark partition và tránh Python serialization. Tuy vậy, kiểm tra type bằng VARIANT cho 32 field làm tăng CPU; cần đo `processedRowsPerSecond` trước khi tối ưu. Với producer hiện tại, correctness quan trọng hơn micro-optimization.

### Observability

Mỗi micro-batch tương lai phải cung cấp:

- input/VALID/WARNING/INVALID count.
- DLQ rate và warning rate.
- count theo stable rule ID.
- batch duration, input rows/sec, processed rows/sec.
- Kafka offset range theo partition.

Không log toàn bộ `raw_payload` vì tạo log volume lớn và có thể lộ `reviewerName`.

### Security/privacy

`reviewerName` dù là dữ liệu giả lập vẫn được đối xử như PII: bucket không public, không đưa raw name lên dashboard và không ghi payload đầy đủ vào log. Silver/Gold sẽ xem xét mask hoặc loại trường này.

## 9. Test gate trước khi streaming

Chạy validator ở batch mode trên ít nhất:

1. `valid-event.json` → `VALID`.
2. `warning-event.json` → `WARNING`, có `W101`/`W102` phù hợp.
3. `invalid-dlq-event.json` → `INVALID`, có lỗi required null cho `asin`.
4. Một malformed JSON → `INVALID/E001` nhưng job không crash.
5. Một record thiếu key → `INVALID/E003`.
6. Một record sai type → `INVALID/E005`.

Chỉ sau khi kết quả deterministic mới ghép cùng Kafka stream và thiết kế S3A sink.

### Implementation artifacts

- `spark/pipeline/schema.py`: schema và danh sách field dùng lại được.
- `spark/pipeline/validation.py`: transformation `validate_events(df)` bằng native expressions.
- `spark/checks/contracts/validate_contract_samples.py`: regression harness sáu case.

Khác smoke test hạ tầng đã xóa, regression harness này được giữ trong repository. Nó là safety net cho contract/rule khi pipeline được sửa hoặc nâng version.

### Lệnh người dùng tự chạy

Compose đã đổi source mount từ riêng `spark/jobs` sang toàn bộ `spark`, đồng thời mount sample contract. Cập nhật container Spark:

```powershell
docker compose --profile streaming up -d spark-master spark-worker
docker compose --profile streaming ps spark-master spark-worker
```

Chạy regression harness:

```powershell
docker compose --profile streaming exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-memory 1g --total-executor-cores 2 /opt/spark-apps/checks/contracts/validate_contract_samples.py
```

Không cần Kafka connector vì input là sáu case hữu hạn trong contract samples. Kết quả đạt phải có bảng sáu case và marker:

```text
CONTRACT_VALIDATION_OK application_id=... cases=6 statuses=VALID,WARNING,INVALID
```

Nếu một assertion sai, job chủ động trả exit code khác 0. Không sửa expected result để làm test xanh; phải xác định lỗi nằm ở rule, fixture hay assumption về Spark JSON type.

### Runtime checkpoint — đạt

Người dùng đã chạy application `app-20260924045552-0000` và nhận marker:

```text
CONTRACT_VALIDATION_OK application_id=app-20260924045552-0000 cases=6 statuses=VALID,WARNING,INVALID
```

Kết quả từng case:

| Case | Status | Error/warning quan trọng |
|---|---|---|
| `valid` | VALID | Không có error/warning |
| `warning` | WARNING | `W101_UNUSUAL_BRAND_CATEGORY`, `W102_PAYMENT_SHIPPING_INCONSISTENT` |
| `required_null` | INVALID | `E006_REQUIRED_NULL` |
| `malformed` | INVALID | `E001_MALFORMED_JSON`; application không crash |
| `missing_field` | INVALID | `E003_MISSING_FIELDS`, `missing_fields=[orderID]` |
| `wrong_type` | INVALID | `E005_TYPE_MISMATCH` |

Spark warning về execution plan bị rút gọn chỉ ảnh hưởng phần log hiển thị do plan có nhiều expression, không ảnh hưởng kết quả hay status. Regression harness được giữ lại để chạy lại khi đổi contract/rule/Spark version.

Checkpoint này chứng minh validator đúng trên fixture hữu hạn. Nó chưa chứng minh validation trên message Kafka thật, streaming checkpoint, routing vật lý hay ghi MinIO.

### Bounded Kafka integration — runtime đạt

Đã thêm `spark/checks/contracts/validate_kafka_snapshot.py`. Check chụp snapshot Kafka từ `earliest` đến `latest`, giữ metadata lineage, áp dụng chính `validate_events(df)`, rồi kiểm tra:

- số record đầu vào bằng số record đầu ra;
- `(topic, partition, offset)` không trùng;
- tổng ba status khớp tổng output;
- status nhất quán với mảng error/warning;
- có ít nhất một message để tránh test xanh trên input rỗng.

Job chỉ in aggregate và sample lineage không hợp lệ, không log `raw_payload`; chưa ghi MinIO và chưa tạo checkpoint. Xem [bounded Kafka validation runbook](kafka-validation-integration.md).

Runtime application `app-20260924084421-0001` đã kiểm tra 168 Kafka event: input/output/distinct lineage đều bằng 168, phủ đủ 10 partition, gồm 27 VALID, 116 WARNING và 25 INVALID. Toàn bộ INVALID là `E006_REQUIRED_NULL`; không có lỗi structural/type/cross-field ngoài chaos contract đã biết. Marker `KAFKA_VALIDATION_OK` đóng cổng integration này.

## 10. Điều kiện xem xét lại

- Source contract có version mới hoặc cho phép additive field.
- Throughput cho thấy VARIANT validation là bottleneck.
- Cần stateful quality rule hoặc referential check với dimension.
- Cần cùng bộ rule chạy ngoài Spark; khi đó cân nhắc rule engine/schema registry thay vì nhân đôi logic.
