# Redis realtime serving design v1

Ngày chốt thiết kế ban đầu: 2026-09-24.

Đây là thiết kế ban đầu cho project cá nhân. Cấu hình và metric sẽ được xem xét lại sau khi đo runtime; Redis không phải nguồn dữ liệu chuẩn và không thay thế Bronze/Silver/Gold.

## 1. Mục tiêu

Nhánh Redis phục vụ dashboard tức thời với hai nhóm thông tin:

- operational pulse: throughput, VALID/WARNING/INVALID và thời điểm cập nhật cuối;
- business pulse minh họa: số record, sản lượng, giá trị hàng giả lập, giá trị gắn nhãn thanh toán thành công, rating và phân bố trạng thái.

Không gọi `totalAmount` là doanh thu kế toán. Không dùng `reviewTime` làm thời gian bán hàng. Window realtime dùng Kafka timestamp/ingestion time vì nguồn hiện không có order event time.

Freshness mục tiêu v1 là tối đa 60 giây trên laptop. Đây là SLO học tập, chưa phải SLA production.

### Tại sao chọn Redis

Redis được chọn vì workload dashboard là đọc lặp lại một tập nhỏ counter, window theo phút và danh sách event mới nhất. Các cấu trúc HASH/LIST, TTL, thao tác atomic và độ trễ thấp phù hợp hơn việc scan lại file Parquet mỗi lần refresh.

- Kafka là transport/durable event log theo offset, không phải query store cho KPI.
- MinIO/Parquet là storage phục vụ audit, replay và xử lý khối lượng lớn; scan file cho mỗi lần dashboard refresh vừa chậm vừa tốn compute.
- ClickHouse phù hợp cho lịch sử và OLAP Gold ở dữ liệu lớn, nhưng chưa cần thiết cho các key/counter đã pre-aggregate của màn hình tức thời.

Redis không bắt buộc nếu project chỉ làm batch/BI. Nó tồn tại ở kiến trúc này để chứng minh realtime serving: Spark materialize kết quả đã tính sang read model chuyên biệt, còn durable truth vẫn ở lake. Vì Redis là derived state, việc mất Redis không được làm mất hoặc chặn dữ liệu Bronze/DLQ.

## 2. Ranh giới hệ thống

Chọn một Spark Structured Streaming application độc lập đọc Kafka, dùng cùng validator nhưng có checkpoint Redis riêng:

```text
Kafka ──> Kafka-to-Bronze/DLQ job ──> MinIO durable lake
   └────> Kafka-to-Redis job ───────> Redis serving/cache ──> dashboard
```

Không ghi Redis trong hai query Bronze/DLQ hiện tại. Nhờ vậy Redis chậm, hết bộ nhớ hoặc restart không chặn durable ingestion. Trade-off là Kafka được đọc và validation được chạy thêm một lần; chi phí này chấp nhận được ở volume local.

Redis là derived state. Bronze/DLQ và Kafka lineage mới là bằng chứng dữ liệu bền. Nếu Redis mất state sau khi checkpoint đã tiến, phải rebuild từ Bronze thay vì lùi checkpoint tùy tiện.

## 3. Event được tính

- VALID và WARNING được tính vào business pulse.
- INVALID chỉ tăng quality counter; không cộng quantity, monetary value hoặc rating.
- Lineage kỹ thuật là `topic-partition-offset`.
- Không lưu `reviewerName`, `reviewText`, raw payload hoặc dữ liệu nhận diện không cần thiết trong Redis.

## 4. Redis key model

Tất cả key có prefix/version `ecom:rt:v1` để hỗ trợ schema evolution và xóa/rebuild có phạm vi.

| Key | Type | Nội dung | Retention |
|---|---|---|---|
| `ecom:rt:v1:summary` | HASH | cumulative counters và `last_updated_at` | Không TTL; rebuild được |
| `ecom:rt:v1:minute:<yyyyMMddHHmm>` | HASH | metric theo phút ingestion | 48 giờ |
| `ecom:rt:v1:latest` | LIST | tối đa 100 event đã sanitize | Bounded bằng trim |
| `ecom:rt:v1:batch:<batch_id>` | STRING | idempotency marker của `foreachBatch` | 48 giờ |
| `ecom:rt:v1:health` | HASH | last successful batch, latency và error state | Không TTL |

`minute_buckets` là số phút ingestion khác nhau xuất hiện trong snapshot/batch, không phải số Spark micro-batch. Mỗi giá trị `yyyyMMddHHmm` trở thành một HASH riêng để dashboard đọc chuỗi thời gian mà không scan event thô.

`latest` chỉ là read model giới hạn 100 record VALID/WARNING mới nhất theo Kafka timestamp, với partition/offset làm tie-break kỹ thuật. Khi có hơn 100 record, phần cũ bị trim khỏi Redis nhưng vẫn còn ở Kafka theo retention và ở Bronze lâu dài. Danh sách không chứa raw payload, reviewer name/text hoặc summary; nó cũng không đại diện thứ tự business toàn cục vì nguồn không có order event time.

Metric minute gồm:

- `events_total`, `valid_count`, `warning_count`, `invalid_count`;
- `quantity_sum`, `total_amount_sum`, `paid_amount_sum` chỉ từ VALID/WARNING;
- `rating_sum`, `rating_count` để dashboard tự tính average chính xác;
- field đếm có cardinality hữu hạn cho payment status, shipping status, category và region.

Không tạo key theo `orderID`, `reviewerID` hoặc product ID vì cardinality tăng không giới hạn.

## 5. Idempotency và failure semantics

Spark dùng `foreachBatch`. Mỗi micro-batch tổng hợp dữ liệu trước khi gửi sang Redis. Một Lua script thực hiện atomically:

1. kiểm tra `ecom:rt:v1:batch:<batch_id>`;
2. nếu marker đã tồn tại thì bỏ qua toàn bộ batch;
3. nếu chưa tồn tại thì cập nhật metric/list, ghi health và tạo marker trong cùng script.

Điều này bảo vệ trường hợp Redis đã ghi thành công nhưng Spark driver lỗi trước khi checkpoint commit và retry cùng batch ID.

Giới hạn: hai hệ thống Redis và Spark checkpoint không có distributed transaction. Redis mất toàn bộ state sau khi Spark checkpoint đã tiến sẽ không tự được bù; recovery đúng là rebuild Redis từ Bronze và tạo namespace/version mới.

## 6. Memory, persistence và eviction

- Giới hạn memory khởi điểm: 256 MiB.
- Policy: `noeviction` để lỗi ghi hiển thị rõ, tránh âm thầm evict idempotency marker hoặc metric.
- Minute key và batch marker có TTL 48 giờ; latest list luôn trim còn 100 phần tử.
- Dùng named volume và AOF `everysec` cho restart local thuận tiện; vẫn coi Redis là cache có thể rebuild.

Trade-off: `noeviction` ưu tiên correctness/observability hơn availability. Nếu memory đầy, serving job fail rõ ràng trong khi Bronze/DLQ tiếp tục chạy độc lập.

## 7. Security và network

- Chỉ publish Redis về `127.0.0.1:6379` cho dashboard Windows; Spark dùng hostname nội bộ `redis:6379`.
- Credential lấy từ environment, không hard-code vào job hoặc tài liệu runtime.
- Dashboard chỉ đọc metric/sanitized latest events, không đọc raw payload.

## 8. Runtime gates

Thực hiện tuần tự:

1. Redis container healthy, authentication và persistence qua container recreation.
2. Spark → Redis connectivity smoke test có cleanup.
3. Unit test Lua idempotency: cùng batch ID áp dụng hai lần nhưng counter chỉ tăng một lần.
4. Streaming job xử lý controlled input và dashboard keys khớp expected metrics.
5. Restart job không double count.
6. Dừng Redis trong khi streaming để chứng minh Bronze/DLQ không bị ảnh hưởng và serving job fail rõ ràng.

Chỉ sau gate 4–5 mới xây dashboard realtime tối thiểu.

Image v1 được pin `redis:8.10.2-alpine` từ Docker Official Image, không dùng tag `latest` hoặc floating major tag. Redis chỉ publish lên loopback `127.0.0.1` cho môi trường local.

## 9. Trade-off đã chốt cho v1

| Quyết định | Lợi ích | Chi phí/giới hạn |
|---|---|---|
| Job Redis độc lập | Cô lập failure khỏi durable lake | Đọc Kafka và validate thêm lần nữa |
| Aggregate theo ingestion minute | Có window realtime nhất quán | Không phải thời gian bán hàng thật |
| Redis derived state | Có thể reset/rebuild | Cần thiết kế rebuild riêng |
| Lua + batch marker | Retry idempotent trong Redis | Không tạo transaction với Spark checkpoint |
| Bounded keys + noeviction | Memory dự đoán được, không mất key âm thầm | Serving write sẽ fail khi chạm giới hạn |
| Parquet/Bronze vẫn là durable source | Audit/rebuild rõ ràng | Dashboard không được coi Redis là source of truth |
