# Source data contract v1 — contract hiện hành

Ngày chốt phạm vi: 2026-09-23. Contract này mô tả đúng payload 32 trường do `create_data.py` hiện tại sinh ra. Mục tiêu là phục vụ project Data Engineering realtime; chưa mô hình hóa vòng đời đơn hàng thực tế.

Artifacts của contract:

- [JSON Schema v1](schemas/ecommerce-event-v1.schema.json)
- [Mẫu hợp lệ](samples/valid-event.json)
- [Mẫu cảnh báo](samples/warning-event.json)
- [Mẫu phải vào DLQ](samples/invalid-dlq-event.json)

## 1. Grain và semantics

Một record là một quan sát giao dịch-review giả lập độc lập, gồm một sản phẩm, một orderID, một paymentID và một shippingID. Mỗi lần gọi generator tạo ID mới; dữ liệu append-only và không có snapshot/update cho cùng đơn.

`unixReviewTime` và `reviewTime` là thời gian review trong giai đoạn 2023–2024. Generator hiện dùng timezone của máy producer khi chuyển epoch thành `reviewTime`; môi trường local v1 chốt rõ timezone nguồn là `Asia/Ho_Chi_Minh`. Spark phải dùng cùng timezone khi kiểm tra `E206_REVIEW_TIME_MISMATCH`, nếu không record gần nửa đêm có thể bị gắn lỗi giả. Kafka timestamp/ingestion timestamp thể hiện lúc pipeline nhận record. Contract không có ngày đặt hàng, ngày thanh toán hoặc ngày giao hàng.

## 2. Payload 32 trường

| Nhóm | Trường | Kiểu nguồn | Null | Ý nghĩa/điều kiện |
|---|---|---|---|---|
| Review | reviewerID | string | Có chủ đích | ID người review; null được chaos injection tạo để kiểm thử DLQ |
| Review | asin | string | Có chủ đích | ID sản phẩm; null được chaos injection tạo để kiểm thử DLQ |
| Review | reviewerName | string | Có, hợp lệ | Tên hiển thị tùy chọn |
| Review | helpful | array[integer, integer] | Không | Phải bằng `[helpful_yes, total_vote]` |
| Review | reviewText | string | Không | Nội dung giả lập |
| Review | overall | number | Có chủ đích | Rating 1–5; null được chaos injection tạo để kiểm thử DLQ |
| Review | summary | string | Không | Tóm tắt review |
| Review | unixReviewTime | integer | Không | Unix timestamp của review |
| Review | reviewTime | string | Không | Dạng `MM DD, YYYY`, phải khớp unixReviewTime theo cách generator tạo |
| Review | day_diff | integer | Không | Giá trị giả lập 4–1064, chưa có nghĩa business; không dùng làm KPI chính |
| Review | helpful_yes | integer | Không | Số vote hữu ích, từ 0 đến total_vote |
| Review | total_vote | integer | Không | Tổng vote, từ 0 đến 500 |
| Product | productName | string | Không | Tên ghép từ sub-category, brand và từ giả lập |
| Product | category | string | Không | Điện tử, Thời trang, Gia dụng hoặc Mỹ phẩm |
| Product | sub_category | string | Không | Phải thuộc category theo dictionary trong generator |
| Product | brand | string | Không | Một giá trị trong BRANDS; chưa cam kết phù hợp category |
| Product | unitPrice | number | Không | Giá trị từ 50.000 đến 5.000.000 VND, được JSON hóa dưới dạng số |
| Order | orderID | string | Không | ID mới cho mỗi record |
| Order | quantity | integer | Không | Từ 1 đến 4 |
| Order | totalAmount | number | Không | Phải bằng unitPrice × quantity |
| Store | storeID | string | Không | ID cửa hàng giả lập; chưa đủ ổn định để làm master data chuẩn |
| Store | storeName | string | Không | Tên cửa hàng giả lập |
| Store | city | string | Không | Phải thuộc region theo dictionary nguồn |
| Store | region | string | Không | Miền Bắc, Miền Trung hoặc Miền Nam |
| Store | storeType | string | Không | Cửa hàng Vật lý, Web Online hoặc Mobile App |
| Payment | paymentID | string | Không | ID thanh toán mới cho mỗi record |
| Payment | paymentMethod | string | Không | COD, Thẻ Tín Dụng, Momo, ZaloPay hoặc VNPay |
| Payment | paymentStatus | string | Không | Thành công, Đang xử lý hoặc Thất bại |
| Shipping | shippingID | string | Không | ID vận chuyển mới cho mỗi record |
| Shipping | shippingMethod | string | Không | Hỏa tốc, Tiêu chuẩn hoặc Tiết kiệm |
| Shipping | carrierName | string | Không | Một giá trị trong CARRIERS |
| Shipping | shippingStatus | string | Không | Đã giao, Đang vận chuyển hoặc Hoàn hàng |

Python `float` của `unitPrice`, `totalAmount` và `overall` được JSON hóa thành JSON number. Silver sẽ ép `unit_price`/`total_amount` sang decimal phù hợp và `overall` sang số nguyên sau khi validation.

## 3. Phân loại validation

JSON Schema kiểm tra sự hiện diện, kiểu, pattern, enum, range và quan hệ category/sub-category, region/city. Validator quality trong Spark kiểm tra các phép so sánh giữa trường như tổng tiền, helpful và sự khớp nhau của hai dạng thời gian; đồng thời gắn cảnh báo business. Không coi JSON Schema là toàn bộ hệ thống kiểm tra chất lượng.

### DLQ — record không được vào Silver

- JSON không parse được, payload không phải object, thiếu một trong 32 trường hoặc có trường sai kiểu.
- `reviewerID`, `asin` hoặc `overall` là null. Đây là lỗi có chủ đích từ `inject_chaos`, dự kiến tổng cộng khoảng 15% record.
- Giá trị ngoài enum/range mà generator cam kết.
- `totalAmount != unitPrice * quantity`.
- `helpful != [helpful_yes, total_vote]` hoặc `helpful_yes > total_vote`.
- `sub_category` không thuộc `category`, hoặc `city` không thuộc `region`.
- `reviewTime` không biểu diễn cùng ngày với `unixReviewTime`.

### Hợp lệ

- `reviewerName = null` là hợp lệ và không tạo cảnh báo.
- Các trường và quan hệ mà generator cam kết đều đúng.

### Cảnh báo — vẫn được vào Bronze/Silver

- Brand không tự nhiên với category, ví dụ Laptop Nike.
- Tổ hợp paymentStatus/shippingStatus thiếu hợp lý theo business.
- Cùng storeID xuất hiện với storeName hoặc city khác.
- Review date cũ hơn nhiều so với ingestion timestamp là metric về source age, không đổi status từng record trong validator v1 vì nguồn cố ý chỉ sinh ngày 2023–2024.

Các cảnh báo phản ánh giới hạn nguồn hiện tại. Chúng được gắn quality flags để quan sát, không bị coi là sự cố của pipeline. Store consistency cần state/dimension nên được hoãn khỏi stateless validator đầu tiên.

## 4. Metadata do pipeline bổ sung

Payload Kafka giữ nguyên 32 trường. Kafka-to-Bronze bổ sung metadata riêng:

| Trường | Nguồn | Mục đích |
|---|---|---|
| kafka_topic | Kafka | Truy vết topic |
| kafka_partition | Kafka | Truy vết partition |
| kafka_offset | Kafka | Truy vết vị trí duy nhất trong partition |
| kafka_timestamp | Kafka | Thời điểm Kafka ghi record |
| ingested_at | Streaming job | Thời điểm job xử lý record |
| validation_status | Validator | VALID, WARNING hoặc INVALID |
| validation_errors | Validator | Danh sách rule bị vi phạm |
| validation_warnings | Validator | Danh sách cảnh báo không chặn Bronze |
| contract_version | Pipeline | Version contract dùng để parse/validate |
| raw_payload | Kafka value | Giữ payload gốc cho DLQ và audit |

`SOURCE_TIME_ZONE=Asia/Ho_Chi_Minh` là cấu hình semantics của source, không chỉ là cấu hình hiển thị. Nếu producer chuyển sang UTC hoặc timezone khác thì phải đổi contract/config đồng bộ và chạy lại regression gate.

Khóa truy vết kỹ thuật là `(kafka_topic, kafka_partition, kafka_offset)`. Contract không có eventID, nên pipeline không cam kết nhận biết cùng một business payload được producer gửi hai lần với hai offset khác nhau.

## 5. Chỉ số được phép diễn giải

- `record_count`: số record hợp lệ.
- `quantity_sum`: tổng quantity.
- `gross_item_value`: tổng totalAmount của dữ liệu giả lập.
- `successful_labeled_value`: tổng totalAmount có paymentStatus = Thành công.
- Phân bố paymentStatus, shippingStatus, category, region và storeType.
- Rating trung bình và helpful ratio trên record hợp lệ.
- Valid/warning/invalid count, DLQ rate, throughput và processing latency.

Không gọi `successful_labeled_value` là doanh thu kế toán hoặc tiền đã đối soát. Không dùng reviewTime để biểu diễn ngày bán hàng. Không suy luận thời gian chuyển trạng thái vì nguồn chỉ cung cấp một nhãn trạng thái độc lập.

## 6. Trade-off và điều kiện nâng cấp

Giữ nguyên nguồn giúp tập trung vào Kafka, Spark, lakehouse, warehouse và observability. Đổi lại, pipeline chỉ chứng minh năng lực kỹ thuật và phân tích mô tả trên dữ liệu giả lập.

Chỉ nâng cấp source contract khi cần ít nhất một trong các khả năng: đơn nhiều sản phẩm, trạng thái thay đổi theo thời gian, business event time, deduplication xuyên offset, thanh toán/giao hàng nhất quán hoặc KPI tài chính. Khi đó tạo contract phiên bản mới; không âm thầm thay nghĩa của 32 trường v1.

## 7. Runtime validation checkpoint

Ngày 2026-09-24, Spark application `app-20260924045552-0000` đã kiểm chứng contract trên sáu case: VALID, WARNING, required null, malformed JSON, missing field và wrong type. Marker `CONTRACT_VALIDATION_OK` xác nhận routing/status và stable rule ID đúng như thiết kế. Đây là batch regression trên fixtures; tại checkpoint này Kafka payload thật và Bronze/DLQ physical routing chưa được kiểm chứng.

Sau đó application `app-20260924084421-0001` kiểm chứng 168 payload Kafka thật: 168 output và 168 lineage duy nhất trên đủ 10 partition. Kết quả 27 VALID, 116 WARNING, 25 INVALID; toàn bộ INVALID là required null do chaos injection. Contract tương thích với source snapshot này; Bronze/DLQ physical routing vẫn chưa được triển khai.
