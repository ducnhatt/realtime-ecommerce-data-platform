# Local environment design v1

Ngày: 2026-09-23. Tài liệu này chốt cách bố trí môi trường local trước khi viết Docker Compose.

## 1. Hiện trạng đã kiểm tra

| Thành phần | Hiện trạng |
|---|---|
| Windows host | Workspace nằm trên ổ D: |
| Python | 3.10.11 |
| Project venv | Python 3.10.11 |
| Docker CLI | 28.5.1 |
| Docker Compose | 2.40.0 |
| Docker Desktop engine | Chưa chạy tại thời điểm kiểm tra |
| Java host | Chưa cài/không có trên PATH |
| RAM/CPU/disk | Sandbox không đọc được; cần kiểm tra trước khi bật nhiều profile |

Không yêu cầu cài Java trên Windows. Kafka và Spark sẽ dùng Java bên trong container.

## 2. Phân chia host và Docker

| Thành phần | Nơi chạy | Lý do | Trade-off |
|---|---|---|---|
| `create_data.py` producer | Windows + venv | Dễ debug, giữ môi trường hiện tại và kết nối `localhost:9092` | Phải duy trì dependency Python trên host |
| Kafka KRaft | Docker | Không cần Java host, dễ tái lập | Một broker local không kiểm thử HA |
| Spark master/worker và streaming jobs | Docker | Đồng nhất Java/Spark/connector | Tốn RAM hơn local process, debug container phức tạp hơn |
| MinIO | Docker | Cung cấp S3-compatible API cho Bronze/Silver/DLQ | Single-node chỉ phục vụ development, không có độ bền production |
| Redis | Docker | Windows được Redis hướng dẫn dùng Docker; dễ reset dữ liệu realtime | Dữ liệu cache có thể mất nếu không bật persistence |
| ClickHouse | Docker | OLAP columnar local, tái lập database/volume và tách khỏi máy host | Cần quản lý merge, part, RAM và backup local |
| dbt Core | venv riêng trên host | Chạy CLI và sửa model nhanh | Phải khóa dependency riêng với producer |
| Streamlit | venv riêng hoặc cùng analytics venv trên host | Vòng lặp phát triển UI nhanh | Host có thêm dependency |
| Airflow | Docker profile riêng | Airflow có nhiều process/dependency, không nên trộn vào venv producer | Stack nặng; chỉ bật khi đến giai đoạn orchestration |
| Power BI Desktop | Windows host | Công cụ desktop native | Không có publish/refresh cloud tự động |

Không dùng chung một Python environment cho producer, dbt/Airflow và dashboard. Tách môi trường tránh xung đột dependency và làm rõ ownership.

## 3. Compose topology theo giai đoạn

Một file Compose có thể dùng profiles để chỉ bật nhóm service cần thiết:

| Profile | Service dự kiến | Khi sử dụng |
|---|---|---|
| mặc định/core | Kafka | Producer và kiểm tra message |
| `streaming` | Spark master, Spark worker, MinIO | Kafka-to-Bronze và Bronze-to-Silver |
| `serving` | Redis | Realtime dashboard |
| `warehouse` | ClickHouse | Silver load, dbt và Power BI |
| `orchestration` | Airflow components | DAG, retry, backfill và scheduling |

Docker Compose profiles cho phép giữ một topology nhưng không buộc máy cá nhân chạy toàn bộ service cùng lúc. Trade-off là cần ghi rõ profile nào phải bật cho từng luồng test.

## 4. Lựa chọn phiên bản và format

| Thành phần | Quyết định v1 | Lý do |
|---|---|---|
| Kafka | Official `apache/kafka:4.2.0`, KRaft combined broker/controller | Image chính thức; không cần ZooKeeper; phù hợp single-node local |
| Spark | Docker Official Image `spark:4.0.4-scala2.13-java17-python3-ubuntu` | Spark 4.0.4, Scala 2.13, Java 17 và Python 3 được pin đồng bộ; khớp nhánh Spark được ClickHouse connector công bố hỗ trợ |
| Lake format | Parquet ở Bronze/Silver v1 | Nguồn append-only, triển khai nhẹ và ít dependency |
| MinIO | Community legacy `RELEASE.2025-04-22T22-12-26Z`, single-node | Bản legacy cuối có Console đầy đủ và S3-compatible; dễ trình bày trong project cá nhân |
| Redis | Official image, pin version khi tạo profile | Chỉ là serving/cache local |
| ClickHouse | Pin stable image digest khi tạo profile | Chọn sau compatibility/resource gate; không dùng `latest` |

Không dùng tag `latest` trong Compose đã chốt. Mọi image phải pin tag cụ thể và được ghi trong decision log. Delta Lake được xem xét lại nếu xuất hiện update/upsert, time travel hoặc yêu cầu transaction trên lake.

### Parquet và Delta Lake

**Chọn Parquet cho v1:** dữ liệu nguồn append-only, chưa có snapshot/update và mục tiêu gần nhất là chứng minh Kafka → Spark → object storage.

**Trade-off chấp nhận:** Parquet không tự cung cấp ACID transaction, schema enforcement ở table layer, merge/upsert hoặc time travel. Pipeline phải dựa vào checkpoint và output path để tránh ghi lặp.

**Điều kiện xem xét Delta:** cần cập nhật bản ghi, merge theo business key, quản lý concurrent writes hoặc demo lakehouse transaction.

## 5. Network, listener và storage

- Một bridge network: `ecommerce-net`.
- Kafka có listener nội bộ cho container (`kafka:29092`) và listener ngoài cho producer host (`localhost:9092`).
- Spark dùng listener nội bộ; producer Windows dùng listener ngoài.
- MinIO API: `9000`; console: `9001`.
- Redis: `6379`.
- ClickHouse HTTP/native: `8123`/`9000` (host port phải tránh xung đột MinIO bằng mapping riêng).
- Dùng named volumes cho dữ liệu Kafka, MinIO, Redis, ClickHouse và Airflow metadata.
- Chỉ bind-mount source/config cần sửa từ workspace; dữ liệu lớn không bind trực tiếp vào thư mục source.
- Credentials nằm trong `.env`, còn `.env.example` chỉ chứa giá trị development mẫu không nhạy cảm.

## 6. Tài nguyên và cách chạy tăng dần

Do chưa đọc được RAM/CPU của Docker Desktop, không đặt cấu hình toàn stack ngay. Trình tự bật service:

1. Kafka duy nhất; kiểm tra producer và topic.
2. Thêm MinIO và một Spark worker; đo RAM/CPU và throughput.
3. Thêm Redis khi xây nhánh realtime.
4. Thêm ClickHouse khi Silver ổn định.
5. Chỉ bật Airflow khi các job độc lập đã chạy đúng.

Nếu Docker Desktop có ít hơn khoảng 8 GB RAM được cấp, không bật Spark, ClickHouse và Airflow cùng lúc. Đây là ngưỡng vận hành local ban đầu, sẽ điều chỉnh bằng số liệu thực tế.

## 7. Reliability scope local

- Kafka: một broker, replication factor 1. Kiểm thử restart/recovery, không tuyên bố high availability.
- Spark: một master/một worker. Kiểm thử checkpoint và restart job, không tuyên bố distributed fault tolerance đầy đủ.
- MinIO: single-node. Kiểm thử API/partitioning, không tuyên bố durability như S3.
- MinIO Community đã ngừng được duy trì. Project chủ động chấp nhận trade-off này để học S3 API và dùng giao diện quen thuộc; chỉ chạy local, không public ra Internet và không dùng production. Nếu phạm vi thay đổi, phải đánh giá lại object storage.
- Redis: cache/serving, có thể dựng lại từ pipeline.
- ClickHouse: named volume; backup/restore và part/merge monitoring được bổ sung khi đến warehouse.

Giới hạn này có chủ đích cho máy cá nhân. Giá trị portfolio nằm ở việc nêu rõ phạm vi và chứng minh recovery trong phạm vi đó.

## 8. Bước triển khai tiếp theo

Kafka, Spark và MinIO đã qua các cổng hạ tầng riêng. MinIO init hiện quản lý ba data bucket `bronze`/`silver`/`dlq` và bucket state kỹ thuật `checkpoints`; xem [MinIO local runbook](minio-local-runbook.md). Bước hiện tại là kiểm chứng Structured Streaming ghi Bronze/DLQ và resume từ checkpoint.
