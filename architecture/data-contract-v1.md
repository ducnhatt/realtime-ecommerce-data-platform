# Data contract snapshot v1 — đã hoãn, chỉ tham khảo

> Quyết định hiện hành ngày 2026-09-23: giữ nguyên `create_data.py` và 32 trường nguồn, tập trung project Data Engineering realtime. Contract snapshot bên dưới không còn áp dụng cho triển khai hiện tại. Các trường thêm mới, quy tắc đủ dòng/phiên bản và giới hạn business chỉ được xem xét lại nếu nâng cấp nguồn sau này. Xem [architecture.md, mục 0](architecture.md).

Ngày: 2026-09-23. Business baseline đã thống nhất ở [architecture.md, mục 1.1](architecture.md). Bộ khóa và quy tắc snapshot tại mục 3 đã được thống nhất qua trao đổi. Các giới hạn nghiệp vụ tại mục 8 và chi tiết trường/validation còn là đề xuất để xem xét. Generator chưa thay đổi.

## 1. Grain và phạm vi đề xuất

Một message chứa snapshot đầy đủ của một dòng sản phẩm trong một đơn. Snapshot mới thay thế trạng thái cũ, không phải số lượng/tiền phát sinh thêm. JSON nguồn giữ camelCase để giảm thay đổi so với generator; Silver chuyển sang snake_case.

Để giữ phiên bản đầu đủ nhỏ, đề xuất: mỗi đơn thuộc một cửa hàng và kênh bán, dùng VND, có một kết quả thanh toán cấp đơn và tối đa một vận đơn. Chưa có thanh toán một phần, thuế, giảm giá, phí vận chuyển, đổi giá/số lượng hay xóa dòng sau khi tạo đơn. Các trường hợp này cần mở rộng contract trước khi hỗ trợ.

Trade-off: có thể tính giá trị đặt ban đầu từ các dòng bất biến và không phải phân bổ thanh toán; chưa mô phỏng marketplace nhiều người bán, tách kiện hoặc chỉnh sửa giỏ hàng sau khi đặt.

## 2. Danh sách trường đề xuất

Mọi trường dưới đây có mặt trong payload, kể cả khi giá trị null. String định danh phải khác rỗng. Timestamp là chuỗi ISO 8601 UTC kết thúc bằng `Z`. Tiền là integer VND để tránh sai số float. `ingestedAt` là metadata do ingestion thêm, không phải trường producer bắt buộc gửi.

| Trường | Kiểu / nullable | Ý nghĩa và quy tắc |
|---|---|---|
| schemaVersion | string | `1.0`; phân biệt phiên bản contract |
| eventID | string | ID message; retry cùng message giữ nguyên ID |
| orderID | string | ID ổn định của đơn |
| orderItemID | string | ID dòng; duy nhất trong đơn |
| snapshotVersion | integer > 0 | Phiên bản cấp đơn, tăng khi snapshot thay đổi |
| snapshotAt | timestamp | Thời điểm nguồn quan sát snapshot |
| orderItemCount | integer > 0 | Tổng số dòng trong đơn, không phải tổng quantity |
| orderCreatedAt | timestamp | Thời điểm đặt hàng, bất biến |
| orderStatus | string | `ACTIVE`, `CANCELLED`, `COMPLETED` |
| currency | string | `VND` |
| asin | string | ID sản phẩm, tái sử dụng giữa các đơn; giữ tên cũ, không khẳng định là mã Amazon thật |
| productName | string | Tên sản phẩm |
| category | string | Danh mục lớn |
| sub_category | string | Danh mục con; giữ tên cũ ở nguồn |
| brand | string | Thương hiệu phù hợp danh mục |
| unitPrice | integer > 0 | Giá một đơn vị tại lúc đặt |
| quantity | integer > 0 | Số lượng đặt mua |
| totalAmount | integer > 0 | Thành tiền dòng = unitPrice × quantity |
| orderAmount | integer > 0 | Tổng giá trị hàng toàn đơn; lặp để đối soát, không SUM trên các dòng |
| storeID | string | ID cửa hàng ổn định |
| storeName | string | Tên cửa hàng |
| city | string | Thành phố của cửa hàng, chưa phải địa chỉ nhận hàng |
| region | string | Vùng của cửa hàng |
| storeType | string | `PHYSICAL`, `WEB`, `MOBILE` |
| paymentID | string / null | ID thanh toán cấp đơn, null nếu chưa phát sinh |
| paymentMethod | string | `COD`, `CARD`, `MOMO`, `ZALOPAY`, `VNPAY` |
| paymentStatus | string | `UNPAID`, `PENDING`, `SUCCEEDED`, `FAILED` |
| paidAmount | integer >= 0 | Tiền đã thanh toán cấp đơn; 0 nếu chưa thành công |
| paidAt | timestamp / null | Chỉ có khi thanh toán thành công |
| shippingID | string / null | ID vận đơn cấp đơn, null nếu chưa có |
| shippingMethod | string / null | `EXPRESS`, `STANDARD`, `ECONOMY`; null trước khi chọn |
| carrierName | string / null | Đơn vị vận chuyển; null nếu chưa phân công |
| shippingStatus | string | `NOT_SHIPPED`, `IN_TRANSIT`, `DELIVERED`, `RETURNED` |
| deliveredAt | timestamp / null | Thời điểm giao thành công nếu đã xảy ra |
| reviewID | string / null | ID review, tối đa một review mỗi dòng ở v1 |
| reviewerID | string / null | Người review; không mặc định là ID người mua |
| reviewerName | string / null | Tên hiển thị tùy chọn |
| overall | integer / null | Rating 1–5 khi có review |
| reviewText | string / null | Nội dung, có thể chỉ chấm sao |
| summary | string / null | Tóm tắt tùy chọn |
| reviewedAt | timestamp / null | Thời điểm review |
| helpful_yes | integer / null | Vote hữu ích, >= 0 |
| total_vote | integer / null | Tổng vote, >= helpful_yes |

Không gửi `helpful` vì trùng hai cột vote. Thay `unixReviewTime` và `reviewTime` bằng `reviewedAt`. Bỏ `day_diff` vì chưa có định nghĩa. Việc chuyển payload cũ sang v1 phải được thực hiện có chủ đích; file hiện tại chưa tuân thủ contract này.

## 3. Khóa, snapshot và tính nhất quán

- Khóa dòng nghiệp vụ: `(orderID, orderItemID)`.
- Khóa phiên bản dòng: `(orderID, orderItemID, snapshotVersion)`.
- Khi một đơn thay đổi trạng thái hoặc có review mới, nguồn gửi lại tất cả dòng của đơn ở cùng phiên bản. Mỗi dòng có eventID riêng; retry giữ ID và payload cũ.
- Chỉ công bố phiên bản đơn khi nhận đủ `orderItemCount` dòng phân biệt, trường cấp đơn nhất quán và tổng totalAmount bằng orderAmount. Không ghép dòng phiên bản cũ với dòng phiên bản mới.
- Dùng phiên bản hoàn chỉnh lớn nhất làm trạng thái hiện hành. Bản cũ đến muộn được lưu để truy vết nhưng không ghi đè bản mới.
- Cùng khóa phiên bản nhưng khác payload là conflict cần cách ly; không tùy ý lấy bản đến sau.
- Bản chưa đủ dòng nằm trong vùng chờ; timeout/cách replay sẽ chốt khi triển khai streaming. Chưa đủ dòng không đồng nghĩa schema lỗi.

Lý do: tránh cộng lặp đơn/tiền và tránh báo cáo trạng thái thanh toán không đồng nhất giữa các dòng. Trade-off: phát lại toàn bộ dòng làm tăng số message và cần vùng chờ phiên bản hoàn chỉnh. Snapshot không có nghĩa là mỗi message tự đủ để tính KPI cấp đơn.

## 4. Quy tắc kiểm tra đề xuất

1. Thiếu khóa hoặc sai kiểu/range/enum: không đưa vào KPI; giữ payload và lý do lỗi để điều tra.
2. `orderCreatedAt <= snapshotAt`; mọi thời điểm hành động có giá trị phải nằm trong khoảng này. Không bắt `ingestedAt == snapshotAt`; có thể nhận muộn/replay.
3. Khi `paymentStatus = SUCCEEDED`: paymentID và paidAt có giá trị, paidAmount = orderAmount. Các trạng thái khác: paidAmount = 0 và paidAt null. Payment PENDING/FAILED phải có paymentID.
4. Đơn COMPLETED phải DELIVERED và thanh toán SUCCEEDED. Đề xuất chưa mô phỏng hủy sau thanh toán trong v1 vì hoàn tiền ngoài phạm vi.
5. DELIVERED phải có deliveredAt. RETURNED có thể có hoặc không có deliveredAt tùy đã từng giao thành công hay trả về trước giao; không tự suy ra tiền hoàn.
6. IN_TRANSIT/DELIVERED/RETURNED phải có shippingID, shippingMethod và carrierName. COD cho phép DELIVERED khi tiền chưa thu; không áp quy tắc mọi đơn phải trả tiền trước khi giao.
7. `reviewID = null`: toàn bộ nhóm review null, đây là đơn hợp lệ. Khi có review: reviewerID, overall, reviewedAt và hai cột vote bắt buộc; tên/nội dung/tóm tắt vẫn tùy chọn. Đề xuất review chỉ xuất hiện sau giao thành công.
8. Trường cấp đơn phải giống nhau giữa các dòng cùng phiên bản, gồm trạng thái, cửa hàng, thời gian đơn/thanh toán/giao hàng và số tiền cấp đơn. Product và review là dữ liệu cấp dòng.
9. Giá trị không khớp phải được đánh dấu, không âm thầm tính lại rồi coi nguồn hợp lệ. Registry sản phẩm/cửa hàng của generator phải giữ quan hệ ID và thuộc tính ổn định.

JSON Schema sẽ kiểm tra cấu trúc, nullable và enum. Các phép so sánh giữa trường, giữa message, tổng tiền và tính nhất quán phiên bản cần validator nghiệp vụ riêng.

## 5. Quy tắc tổng hợp KPI

- Chỉ dùng phiên bản hoàn chỉnh hiện hành của từng đơn để tránh cộng lặp snapshot.
- Giá trị đặt hàng/sản lượng: cộng totalAmount/quantity cấp dòng, theo ngày orderCreatedAt; đơn hủy vẫn thuộc tổng đặt ban đầu.
- Số đơn và phân bố trạng thái: đếm đơn ở cấp orderID, không đếm message.
- Chưa thanh toán: tổng orderAmount của đơn ACTIVE có paymentStatus khác SUCCEEDED. Đơn CANCELLED bị loại.
- Tiền thanh toán: paidAmount của từng đơn SUCCEEDED, tính một lần theo paidAt. Không phân bổ tiền cấp đơn xuống sản phẩm trong v1; KPI theo sản phẩm dùng giá trị dòng hàng.
- Rating: một lần mỗi reviewID trong trạng thái hiện hành, theo reviewedAt; đơn không có review không đóng góp vào mẫu số. Review trùng ID ở nhiều dòng phải được phát hiện.
- Báo cáo theo ngày đặt hàng với trạng thái hiện hành khác báo cáo trạng thái "tại cuối ngày trong quá khứ". Loại thứ hai chưa được hỗ trợ ở v1.

## 6. Kịch bản đối soát trước khi duyệt contract

| Trường hợp | Kết quả mong đợi |
|---|---|
| Một đơn: áo 2 × 200.000, giày 1 × 600.000; ACTIVE/UNPAID | 1 đơn, 2 dòng, 3 sản phẩm, đặt hàng 1.000.000, chưa thanh toán 1.000.000, tiền thu 0 |
| Đơn trên có snapshot mới SUCCEEDED | Số đơn/sản lượng/giá trị đặt không tăng; tiền thanh toán 1.000.000, chưa thanh toán 0 |
| Nhận lại toàn bộ snapshot do retry | Các KPI không thay đổi |
| Chỉ nhận 1 trong 2 dòng ở phiên bản mới | Giữ phiên bản hoàn chỉnh trước, chờ dòng còn lại |
| Phiên bản cũ đến sau bản mới | Không làm lùi trạng thái |
| Đơn ACTIVE/UNPAID chuyển CANCELLED | Giá trị đặt ban đầu giữ nguyên, giá trị chưa thanh toán giảm về 0 |
| Đơn không có review | Đơn hợp lệ; không tính rating bằng 0 |
| Một review rating 4 lặp lại trong snapshot | 1 review, rating trung bình 4 |
| COD đã giao nhưng chưa thu tiền | DELIVERED, paidAmount = 0; không coi là tiền thanh toán thành công |
| Tổng tiền các dòng khác orderAmount | Cách ly phiên bản, không công bố KPI sai |
| Hai payload khác nhau cùng khóa phiên bản | Ghi conflict, không chọn tùy ý |

Đây là kịch bản kỳ vọng, chưa phải kết quả kiểm thử đã chạy.

## 7. Điểm cần duyệt trước JSON Schema và generator

Đã thống nhất giao thức snapshot đủ cả đơn và bộ khóa tại mục 3. Các giới hạn bổ sung cần xem xét tiếp: một cửa hàng/một thanh toán/một vận đơn mỗi đơn, VND nguyên, dòng hàng bất biến, review sau giao hàng và enum trạng thái. Chi tiết lý do/trade-off ở mục 8.

Sau khi duyệt, tạo JSON Schema v1 và bộ JSON mẫu hợp lệ/không hợp lệ, đối soát với KPI mẫu rồi mới kết thúc Giai đoạn 1. Freshness, retention, timeout chờ dòng, vị trí vùng chờ và retry cụ thể được chốt ở giai đoạn triển khai tương ứng. Không xem các giá trị chưa quyết định là SLA đã cam kết.

## 8. Giới hạn nghiệp vụ v1 — đề xuất cho bước hiện tại

Mục này là phương án cụ thể để người dùng xem xét; chưa coi giới hạn mới là đã được chấp thuận.

| Quyết định đề xuất | Lý do | Trade-off / điều kiện xem xét lại |
|---|---|---|
| Một đơn thuộc một cửa hàng và một kênh bán | Nhóm chỉ số cấp đơn theo cửa hàng/kênh không cần phân bổ | Chưa hỗ trợ giỏ hàng nhiều người bán |
| Dòng hàng, số lượng, đơn giá và tổng giá trị bất biến sau tạo đơn | Snapshot trạng thái không làm thay đổi giá trị đặt ban đầu | Sửa đơn phải là tính năng mới; không tự mô phỏng bằng xóa/thêm dòng |
| VND nguyên; chỉ tính giá trị hàng, chưa thuế/phí/giảm giá | Công thức tiền dễ đối soát và không có sai số float | Không thể coi orderAmount là hóa đơn thực tế có mọi khoản phí |
| Tối đa một paymentID mỗi đơn; thanh toán toàn bộ hoặc chưa thu | Tính tiền thu ở cấp đơn rõ ràng | Chưa hỗ trợ thanh toán nhiều lần/một phần; retry gửi message không phải thử thanh toán lại |
| Tối đa một shippingID mỗi đơn | Cùng trạng thái giao hàng cho mọi dòng | Chưa hỗ trợ giao từng phần hoặc nhiều kiện |
| Tối đa một review mỗi dòng; chỉ sau giao thành công | Gắn rating với sản phẩm đã nhận và tránh đếm lặp | Chưa hỗ trợ review không mua hàng hoặc nhiều review cho cùng dòng |
| Hoàn hàng vẫn hiển thị; chưa mô phỏng hoàn tiền | Theo dõi giao vận mà không suy diễn giao dịch tài chính | Tiền đã thu không giảm khi RETURNED; chưa tính tiền thu ròng |

### 8.1. Trạng thái đơn và tính nhất quán đề xuất

Ba trạng thái ACTIVE/CANCELLED/COMPLETED trong bản nháp chưa diễn đạt tốt đơn RETURNED. Đề xuất thêm `CLOSED` cho đơn đã đóng sau hoàn hàng để không xếp đơn hoàn về vào giá trị chưa thanh toán còn hiệu lực.

| orderStatus | Ý nghĩa và điều kiện |
|---|---|
| ACTIVE | Đơn còn xử lý, chưa hủy/hoàn; bao gồm COD đã giao nhưng chưa thu tiền |
| CANCELLED | Hủy trước khi gửi hàng và chưa thu tiền: shippingStatus = NOT_SHIPPED, paymentStatus khác SUCCEEDED |
| COMPLETED | Đã giao và đã thanh toán: DELIVERED + SUCCEEDED |
| CLOSED | Đóng sau hoàn hàng: shippingStatus = RETURNED; không khẳng định đã hoàn tiền |

Quan hệ hai chiều đề xuất: RETURNED đi cùng CLOSED; DELIVERED + SUCCEEDED đi cùng COMPLETED. Các snapshot khác phải phù hợp ACTIVE hoặc CANCELLED theo điều kiện trên. Phát sinh hoàn sau khi hoàn tất có thể cập nhật COMPLETED thành CLOSED; lịch sử chuyển trạng thái chưa là KPI v1.

Đề xuất phương thức trả trước (CARD/MOMO/ZALOPAY/VNPAY) chỉ gửi hàng sau SUCCEEDED. COD cho phép IN_TRANSIT hoặc DELIVERED khi UNPAID/PENDING/FAILED. Đây là quy tắc mô phỏng của project, không phải quy tắc chung cho mọi doanh nghiệp.

Payment SUCCEEDED giữ nguyên số tiền/ngày thanh toán trong snapshot sau kể cả khi hàng được trả về; không đổi thành FAILED để mô phỏng hoàn tiền. Hủy sau thanh toán đang ngoài phạm vi.

### 8.2. Review và thời gian

- Không có review: cả nhóm review null; đơn vẫn hợp lệ.
- Có review: `deliveredAt <= reviewedAt <= snapshotAt`; tên, nội dung và tóm tắt có thể null nếu chỉ chấm sao.
- Đơn đã giao rồi hoàn vẫn có thể giữ review đã phát sinh. RETURNED nhưng chưa từng giao (deliveredAt null) không có review.
- V1 đề xuất giữ reviewID và rating/nội dung ổn định sau tạo; vote có thể được cập nhật qua snapshot mới. Sửa/xóa review được xem xét khi có nhu cầu.

### 8.3. Ví dụ để duyệt giới hạn

Đơn có hai dòng: áo 400.000 và giày 600.000, tổng 1.000.000 VND:

1. ACTIVE/UNPAID/NOT_SHIPPED: đặt hàng 1.000.000, chưa thanh toán 1.000.000, tiền thu 0.
2. COD giao thành công nhưng chưa thu: ACTIVE/UNPAID/DELIVERED; tiền thu vẫn 0.
3. Thu đủ tiền: COMPLETED/SUCCEEDED/DELIVERED; tiền thu 1.000.000 tính một lần cho đơn.
4. Hoàn hàng sau đó: CLOSED/SUCCEEDED/RETURNED; giá trị đặt ban đầu và tiền thanh toán thành công giữ nguyên, không kết luận tiền thu ròng.

Nếu COD chưa thu tiền và hàng được trả về: CLOSED/UNPAID/RETURNED; tiền thu 0, không còn nằm trong giá trị chưa thanh toán của đơn ACTIVE.

Sau khi các giới hạn này được duyệt, đồng bộ enum và validation ở mục 2/4 trước khi tạo JSON Schema. Không dùng đồng thời enum cũ và đề xuất mới trong implementation.
