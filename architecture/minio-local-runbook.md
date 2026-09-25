# MinIO local runbook

Ngày: 2026-09-23.

## 1. Mục đích của bước này

MinIO đóng vai trò object storage tương thích S3 trong môi trường local. Kafka giữ event trong thời gian ngắn; MinIO lưu ba vùng dữ liệu và một vùng state kỹ thuật:

| Bucket | Nội dung dự kiến |
|---|---|
| `bronze` | Event hợp lệ được giữ gần nguyên bản từ Kafka |
| `silver` | Dữ liệu đã làm sạch và chuẩn hóa để phân tích |
| `dlq` | Event không đạt validation cùng nguyên nhân lỗi |
| `checkpoints` | State/progress của Spark Structured Streaming, không phải business data |

Bước hiện tại chỉ dựng storage và bucket. Chưa có Spark nên chưa có dữ liệu được chuyển từ Kafka sang MinIO.

## 2. Quyết định phiên bản

- Server và client khởi tạo bucket dùng chung image community build đã pin digest: `ghcr.io/coollabsio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a4938f37f1be1841b8e7b627ad0207b265345fd0d063e42d7410c78af0e63e68`.
- Binary trong image đã được kiểm tra thủ công: MinIO `RELEASE.2025-04-22T22-12-26Z`, `mc` `RELEASE.2025-08-13T08-35-41Z`.
- MinIO chạy single-node, dùng named volume `realtime-ecommerce-minio-data`.
- Đây là MinIO Community legacy, chỉ dùng cho học tập/local; không dùng cho production.

Chọn bản server ngày 2025-04-22 vì đây là bản legacy cuối còn giao diện quản trị đầy đủ. Trade-off là bản này không nhận bản vá mới và có trước security release 2025-10-15. Không đưa dịch vụ ra Internet và không tái sử dụng credentials local ở môi trường khác.

Sau local runtime reset ngày 2026-09-25, Quay trả `401 Unauthorized` cho cả repository server và client, còn Docker Hub legacy không cho pull. Compose chuyển sang image do Coollabs build từ source MinIO và phát hành trên GHCR. Pin digest làm deployment lặp lại đúng artifact đã kiểm tra và image chứa cả `minio` lẫn `mc`, nhưng đây vẫn là third-party build: phù hợp project học tập local, không được coi là lựa chọn production hoặc artifact do MinIO chính thức phát hành.

Image community build không đóng gói `curl` như image legacy trước đó. Healthcheck vì vậy dùng `mc ready` với alias cấu hình qua `MC_HOST_health`; đây là kiểm tra readiness của chính S3 service và không cần cài thêm utility chỉ để probe HTTP.

## 3. Hai container làm gì?

`minio` là server chạy liên tục, cung cấp:

- S3 API nội bộ tại `http://minio:9000` cho Spark container sau này.
- S3 API từ Windows tại `http://localhost:9000`.
- Web Console tại `http://localhost:9001`.

`minio-init` là init container chạy một lần sau khi MinIO healthy. Nó tạo `bronze`, `silver`, `dlq`, `checkpoints` bằng `mc mb --ignore-existing`, in danh sách bucket rồi thoát với mã 0. Cờ `--ignore-existing` giúp chạy lại an toàn.

## 4. Lệnh người dùng tự chạy

Nếu chưa có `.env`, tạo từ file mẫu rồi kiểm tra credentials local trước khi chạy:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Khởi động MinIO và chạy bước tạo bucket:

```powershell
docker compose --profile streaming up -d minio minio-init
```

Xem trạng thái:

```powershell
docker compose --profile streaming ps -a minio minio-init
```

Kết quả mong đợi:

- `minio` ở trạng thái `healthy`.
- `minio-init` ở trạng thái `Exited (0)`; đây là kết quả thành công, không phải lỗi.

Xem log tạo bucket:

```powershell
docker compose --profile streaming logs minio-init
```

Kiểm tra bucket trực tiếp bằng MinIO Client:

```powershell
docker compose --profile streaming run --rm --no-deps --entrypoint /bin/sh minio-init -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls local'
```

Mở `http://localhost:9001`, đăng nhập bằng `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` trong `.env` để kiểm tra bốn bucket.

## 5. Kiểm tra persistence

`docker compose down` chỉ xóa container và network, không xóa named volume. Có thể kiểm tra bằng chuỗi lệnh:

```powershell
docker compose --profile streaming down
docker compose --profile streaming up -d minio
docker compose --profile streaming ps minio
```

Sau đó mở Console và xác nhận các bucket vẫn tồn tại. Không khởi chạy `minio-init` trong lần kiểm tra này, vì nó có thể tạo lại bucket và làm sai ý nghĩa phép thử persistence.

Phải thêm `--profile streaming` vào lệnh `down`. Nếu không, Compose chỉ xóa service mặc định như Kafka, còn MinIO thuộc profile `streaming` tiếp tục chạy; cảnh báo `Network ecommerce-net Resource is still in use` là dấu hiệu của trường hợp này.

Không dùng `docker compose down -v` nếu muốn giữ dữ liệu, vì `-v` xóa named volume MinIO và Kafka.

## 6. Cổng hoàn thành

Bước MinIO đạt khi:

1. Server báo `healthy`.
2. Init container kết thúc với code 0.
3. Console hoặc `mc ls local` hiển thị đủ `bronze`, `silver`, `dlq`, `checkpoints`.
4. Sau `down` rồi `up`, các bucket vẫn còn.

Sau cổng này mới thiết kế Spark Kafka-to-Bronze/DLQ; không thêm Spark trước khi storage được xác minh.

## 7. Runtime checkpoint

Ngày 2026-09-23, người dùng đã xác nhận:

- `minio` chạy ở trạng thái `healthy` trên cổng `9000-9001`.
- `minio-init` kết thúc với `Exited (0)`.
- Log cho thấy tạo thành công `bronze`, `silver`, `dlq`.
- MinIO Console tại `localhost:9001` hiển thị đủ ba bucket.

Khởi tạo storage và bucket đã đạt. Kiểm tra persistence qua `docker compose down` rồi `up` vẫn còn chờ xác nhận.

Ngày 2026-09-24, cấu hình init được mở rộng thêm bucket kỹ thuật `checkpoints` cho Structured Streaming. Bucket mới vẫn chờ runtime log `minio-init` xác nhận; checkpoint lịch sử ba data bucket ở trên không bị viết lại.
