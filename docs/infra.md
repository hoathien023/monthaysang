# Tài liệu hạ tầng Kafka (TV2)

> Bản nháp phục vụ đồ án Nhập môn Dữ liệu lớn. Nội dung được viết theo cấu hình hiện có trong `docker-compose.yml` và `infra/create_topics.py`; nhóm có thể chỉnh lại cách diễn đạt và lập luận sau khi thống nhất.

## 1. Lựa chọn Kafka thay vì chỉ dùng CSV hoặc một DB đơn lẻ

**Lựa chọn:** Dùng Apache Kafka làm nền tảng tiếp nhận và phân phối các sự kiện web, thay vì chỉ ghi sự kiện vào file CSV hoặc ghi trực tiếp vào một cơ sở dữ liệu.

**Lý do:** Dữ liệu của hệ thống gồm nhiều sự kiện như page view, search, click và transaction. Kafka nhận dữ liệu theo luồng, lưu sự kiện trong topic và cho phép consumer đọc độc lập. Producer không cần chờ từng consumer xử lý xong. Cách này cũng thuận tiện để thử nghiệm nhiều consumer và đo throughput, độ trễ.

**Đánh đổi:** Kafka cần chạy broker, cấu hình topic và quản lý consumer group nên phức tạp hơn CSV hoặc DB đơn giản. Với quy mô nhỏ, CSV/DB có thể dễ triển khai và dễ xem dữ liệu hơn. Kafka không thay thế hoàn toàn DB dùng cho truy vấn nghiệp vụ.

## 2. Lựa chọn KRaft thay vì ZooKeeper

**Lựa chọn:** Chạy Kafka 3.8.0 ở chế độ KRaft. Broker hiện tại đồng thời đảm nhiệm vai trò `broker` và `controller`; cấu hình quorum có một voter là `1@kafka:9093`.

**Lý do:** KRaft cho phép Kafka tự quản lý metadata và controller bằng cơ chế Raft, không cần triển khai thêm ZooKeeper. Với phạm vi đồ án và môi trường Docker Compose hiện tại, cách này giảm số thành phần phải cài đặt và vận hành.

**Đánh đổi:** KRaft vẫn cần cấu hình đúng node ID, cluster ID và controller quorum. Cấu hình một controller/broker phù hợp để chạy thử nhưng không có khả năng chịu lỗi khi node duy nhất gặp sự cố.

## 3. Lựa chọn số partition cho từng topic

**Lựa chọn:** Tạo các topic với số partition như sau:

| Topic | Partition | Lý do chọn |
|---|---:|---|
| `web.clickstream` | 6 | Đây là topic nhận các sự kiện truy cập, tìm kiếm, click và add-to-cart, dự kiến có lưu lượng lớn. Sáu partition tạo thêm khả năng phân phối tải và phù hợp các mức song song 1, 2, 3 hoặc 6 consumer trong thử nghiệm. |
| `web.transactions` | 3 | Lưu lượng giao dịch dự kiến thấp hơn clickstream. Ba partition đủ để thử nghiệm consumer song song và vẫn phân nhóm sự kiện theo `user_id`. Cấu hình này cũng tương ứng với mức `bench.p3` để tiện so sánh. |
| `web.events.dlq` | 1 | DLQ chỉ nhận các sự kiện bị từ chối hoặc xử lý lỗi nên lưu lượng thấp. Một partition giúp việc kiểm tra và đối chiếu dữ liệu lỗi đơn giản hơn. |
| `bench.p1` | 1 | Mốc cơ sở của thí nghiệm E1, dùng để đo trường hợp chỉ có một partition. |
| `bench.p3` | 3 | Mức trung gian của E1, dùng so sánh với cấu hình 1 và 6 partition. |
| `bench.p6` | 6 | Mức cao trong E1, dùng quan sát ảnh hưởng của việc tăng partition đến throughput. |

**Lý do:** Partition cho phép Kafka phân chia dữ liệu trong một topic và phân công các phần dữ liệu cho consumer trong cùng group. Việc dùng các mức 1, 3 và 6 cũng giúp nhóm so sánh ảnh hưởng của số partition trong thí nghiệm E1.

**Đánh đổi:** Tăng partition có thể tăng khả năng xử lý song song, nhưng không đồng nghĩa throughput luôn tăng tuyến tính. Nhiều partition hơn cũng làm tăng metadata và chi phí quản lý. Trong một consumer group, số consumer đang hoạt động nhận dữ liệu của một topic không thể vượt quá số partition của topic đó.

## 4. Lựa chọn key là `user_id`

**Lựa chọn:** Dùng `user_id` làm message key cho các sự kiện gắn với người dùng.

**Lý do:** Kafka sử dụng key để quyết định partition đích. Khi cùng một `user_id` được gửi với cùng cách phân vùng, các sự kiện của người dùng đó sẽ đi vào cùng partition. Vì Kafka giữ thứ tự trong một partition, cách này hỗ trợ giữ thứ tự tương đối của sự kiện theo từng người dùng khi đọc.

**Đánh đổi:** Phân vùng theo `user_id` không đảm bảo thứ tự toàn cục giữa các người dùng hoặc giữa các partition. Nếu một số người dùng tạo dữ liệu nhiều hơn hẳn, tải có thể bị lệch ở một vài partition. Sự kiện thiếu `user_id` cũng cần được xử lý riêng, ví dụ chuyển vào luồng lỗi/DLQ theo logic của ứng dụng.

## 5. Lựa chọn retention 168 giờ

**Lựa chọn:** Đặt `KAFKA_LOG_RETENTION_HOURS=168`, tức Kafka giữ dữ liệu theo thời gian khoảng 7 ngày.

**Lý do:** Bảy ngày tạo khoảng thời gian đủ để nhóm chạy lại consumer, kiểm tra dữ liệu và thực hiện các thí nghiệm mà không phải giữ log vô thời hạn. Cấu hình này được khai báo trong `docker-compose.yml`.

**Đánh đổi:** Dữ liệu cũ có thể bị Kafka xóa sau thời hạn retention, vì vậy không nên xem Kafka là nơi lưu trữ lâu dài duy nhất. Nếu cần lưu kết quả lâu hơn, nhóm phải xuất dữ liệu sang nơi lưu trữ khác. Việc xóa log được thực hiện theo segment và chính sách retention của Kafka, không nhất thiết đúng ngay tại thời điểm đủ 168 giờ.

## 6. Lựa chọn replication factor = 1

**Lựa chọn:** Đặt replication factor bằng 1 cho các topic do `infra/create_topics.py` tạo. Các cấu hình replication của topic nội bộ như `__consumer_offsets` và `__transaction_state` cũng được đặt bằng 1.

**Lý do:** Môi trường hiện tại chỉ có một Kafka broker. Replication factor không thể lớn hơn số broker có thể lưu bản sao. Vì vậy, giá trị 1 là phù hợp để tạo topic và chạy đồ án trên một broker.

**Đánh đổi:** Mỗi partition chỉ có một bản sao trên broker hiện tại. Nếu broker hoặc ổ đĩa chứa dữ liệu bị lỗi, Kafka không có replica khác để tiếp tục phục vụ hoặc khôi phục dữ liệu. Replication factor = 1 không cung cấp khả năng dự phòng dữ liệu.

## 7. Hạn chế khi chỉ chạy một broker

**Lựa chọn:** Dùng một broker cho môi trường phát triển và thực nghiệm của đồ án.

**Lý do:** Một broker giúp cấu hình Docker Compose gọn, ít tốn tài nguyên và đủ để nhóm kiểm tra producer, consumer, partition, key, DLQ và các thí nghiệm hiệu năng cơ bản.

**Đánh đổi:** Hệ thống có điểm lỗi đơn (single point of failure). Khi broker dừng, producer và consumer không thể tiếp tục trao đổi dữ liệu; replication factor = 1 cũng đồng nghĩa không có broker khác giữ bản sao để thay thế. Môi trường này phù hợp cho học tập/thử nghiệm, chưa phải cấu hình triển khai có tính sẵn sàng cao. Muốn tăng khả năng chịu lỗi cần bổ sung nhiều broker, tăng replication factor phù hợp và cấu hình quorum/controller tương ứng.

---

## Ghi chú khi hoàn thiện

- Các số partition ở trên lấy từ `TOPIC_SPECS` trong `infra/create_topics.py`.
- Retention 168 giờ và chế độ KRaft lấy từ `docker-compose.yml`.
- `user_id` là key được dùng trong luồng producer hiện có; cần giữ thống nhất giữa producer và consumer.
- Đây là bản nháp giải thích lựa chọn và đánh đổi, không phải kết quả đo hiệu năng. Nhóm nên bổ sung số liệu thực nghiệm nếu muốn kết luận về throughput hoặc độ trễ.
