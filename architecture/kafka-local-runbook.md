# Kafka local runbook

Kafka local dùng official image `apache/kafka:4.2.0`, một node combined broker/controller ở chế độ KRaft.

## Listener

| Client | Bootstrap server |
|---|---|
| Producer chạy trên Windows | `localhost:9092` |
| Spark hoặc service trong Compose network | `kafka:29092` |

Không dùng `localhost:9092` từ container khác vì `localhost` bên trong container trỏ về chính container đó.

## Lần chạy đầu

Các lệnh dưới đây dành cho người dùng tự chạy trong PowerShell tại thư mục project:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up -d kafka
docker compose ps
```

Chờ Kafka có trạng thái `healthy`, sau đó kiểm tra topic:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

Chạy producer từ venv:

```powershell
python -m create_data --mode realtime
```

`create_data.py` sẽ kiểm tra và tạo topic `ecommerce_reviews` với 10 partition, replication factor 1 và retention một ngày nếu topic chưa tồn tại.

Kiểm tra topic sau khi producer đã chạy:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic ecommerce_reviews
```

Đọc thử message trực tiếp:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic ecommerce_reviews --from-beginning --max-messages 3
```

## Dừng và khởi động lại

Dừng Kafka nhưng giữ container và volume:

```powershell
docker compose stop kafka
```

Khởi động lại:

```powershell
docker compose start kafka
```

Gỡ container/network nhưng giữ named volume:

```powershell
docker compose down
```

Không thêm `--volumes` nếu muốn giữ dữ liệu Kafka.

## Phạm vi và trade-off

- Single-node và replication factor 1 phù hợp local development, không có high availability.
- PLAINTEXT không có authentication/encryption, chỉ dùng trên máy cá nhân.
- Kafka data nằm trong named volume `realtime-ecommerce-kafka-data` và tồn tại qua `docker compose down` thông thường.
- Auto topic creation bị tắt để topic/config không xuất hiện âm thầm. Producer hiện chịu trách nhiệm tạo topic.
- `localhost:9092` dành cho host và `kafka:29092` dành cho container để tránh lỗi advertised listener khi thêm Spark.

## Kết quả kiểm chứng runtime — 2026-09-23

Người dùng đã tự chạy Kafka và producer, sau đó cung cấp log xác nhận:

- Producer kết nối được từ Windows qua `localhost:9092`.
- `create_data.py` tạo topic `ecommerce_reviews` thành công.
- Producer chạy liên tục ở nhịp khoảng 0,5 giây mỗi record.
- Topic có 10 partition, replication factor 1 và `retention.ms=86400000`.
- Cả 10 partition đều có leader 1, replica 1 và ISR 1 tại thời điểm kiểm tra.
- Người dùng đã chạy `docker compose down` sau khi lấy log; named volume không bị yêu cầu xóa.

### Kiểm chứng sau restart

Người dùng đã chạy lại Kafka sau `docker compose down` và cung cấp kết quả:

- Container trở lại trạng thái `healthy` với port host `9092` được publish.
- Console consumer đọc thành công ba JSON cũ bằng internal listener `kafka:29092`.
- Consumer báo `Processed a total of 3 messages`.
- Điều này xác nhận đường đi producer → Kafka → consumer và named-volume persistence qua `down → up`.

Ba payload cũng xác nhận cách phân loại quality warning của source contract là cần thiết: có tổ hợp `Quần Jeans + Dyson`, `Nước hoa + Nike`, và record `paymentStatus = Thất bại` trong khi `shippingStatus = Đang vận chuyển`. Các record này đúng schema nguồn nên vẫn đi tiếp với cảnh báo, không vào DLQ.

**Kết luận:** cổng Kafka core đã đạt. Log được tóm tắt thay vì chép toàn bộ output terminal; đây là bằng chứng kiểm thử thủ công, chưa phải automated integration test.
