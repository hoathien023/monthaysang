# Kết quả thí nghiệm của TV1: E2, E3, E7

Nguồn: `results/E2.csv`, `E3.csv`, `E7.csv` (bản chạy lại bằng 127.0.0.1) và `E7_localhost.csv` (bản chạy đầu).
Mỗi cấu hình chạy 3 lần, mỗi lần 20 giây, trên một máy Windows duy nhất (Producer, API, client tải và Kafka trong Docker dùng chung máy).
Tổng số sự kiện đã gửi trong E2 và E3 là hơn 36,8 triệu.

## E2. Ảnh hưởng của số producer (topic 6 partition)

| Producer | TB sự kiện/giây | Min – Max | Gấp so với 1 producer | Ack p50 (ms) | Ack p95 (ms) | Ack p99 (ms) |
|---|---|---|---|---|---|---|
| 1 | 38.201 | 35.564 – 39.714 | 1,0 | 37 | 83 | 449 |
| 2 | 67.290 | 59.187 – 75.756 | 1,76 | 39 | 78 | 453 |
| 4 | 79.154 | 66.728 – 94.156 | 2,07 | 49 | 133 | 534 |

Nhận xét:
- Từ 1 lên 2 producer, thông lượng tăng khoảng 1,8 lần, gần tuyến tính. Từ 2 lên 4 producer chỉ tăng thêm khoảng 18%, và độ trễ xác nhận p95 tăng từ 78 lên 133 ms.
- Đây là dấu hiệu chạm giới hạn của máy thử nghiệm (các producer, Kafka trong Docker và bộ sinh dữ liệu dùng chung CPU). Chưa tách được nguyên nhân chính xác.
- Ở 4 producer, ba lần chạy dao động rộng (khoảng 66.700 đến 94.200 sự kiện/giây), nên con số 79.154 chỉ nên dùng làm mức tham khảo.
- Không có sự kiện nào bị lỗi gửi (failed = 0).

## E3. Ảnh hưởng của cấu hình Producer (1 producer, topic 6 partition, gửi hết tốc độ)

| Cấu hình | TB sự kiện/giây | Min – Max | Ack p50 | Ack p95 | Ack p99 | Tỉ lệ xác nhận | Thời gian (s) |
|---|---|---|---|---|---|---|---|
| Gốc (batch 64K, linger 5ms, không nén, acks=all) | 43.403 | 32.478 – 49.414 | 36 | 72 | 431 | 100,0% | 20 |
| batch.size = 16K | 9.762 | 8.686 – 10.666 | 469 | 40268 | 63365 | 87,7% | 82 |
| batch.size = 256K | 48.392 | 41.563 – 52.433 | 34 | 63 | 370 | 100,0% | 20 |
| linger.ms = 0 | 51.072 | 50.833 – 51.356 | 36 | 140 | 425 | 100,0% | 20 |
| linger.ms = 20 | 52.377 | 52.222 – 52.533 | 18 | 33 | 406 | 100,0% | 20 |
| nén lz4 | 53.200 | 52.158 – 54.306 | 37 | 310 | 415 | 100,0% | 20 |
| nén zstd | 49.164 | 48.463 – 50.111 | 44 | 608 | 657 | 100,0% | 21 |
| acks = 0 | 46.790 | 43.934 – 48.685 | 8 | 15 | 18 | 100,0% | 20 |
| acks = 1 | 35.752 | 33.547 – 39.295 | 14 | 25 | 30 | 100,0% | 20 |

Nhận xét:
- **batch.size = 16K là bất thường:** ba lần chạy kéo dài khoảng 82 giây (thay vì 20 giây) và chỉ khoảng 87,7% sự kiện được Kafka xác nhận trong thời gian chờ 60 giây. Batch nhỏ làm số request tới Kafka quá nhiều, hàng đợi cục bộ phình ra. Con số thông lượng của dòng này không so sánh trực tiếp được với các dòng khác vì thời gian chờ nằm trong mẫu số.
- Cấu hình gốc dao động lớn giữa các lần chạy (khoảng 32.500 đến 49.400 sự kiện/giây). Vì vậy chênh lệch thông lượng giữa batch 256K, linger 0 và 20, nén lz4 và cấu hình gốc nằm trong khoảng nhiễu, chưa đủ cơ sở kết luận cấu hình nào nhanh hơn.
- Nén lz4 và zstd không làm tăng thông lượng, còn làm đuôi độ trễ xấu hơn (zstd có p95 khoảng 600 ms). Có thể do chạy trên cùng một máy, băng thông không phải nút thắt nên nén chỉ tốn thêm CPU (chưa kiểm chứng).
- Độ trễ ở E3 đo trong lúc Producer gửi hết tốc độ, đã gồm thời gian chờ trong hàng đợi cục bộ. Vì vậy linger 20 ms có độ trễ thấp hơn linger 5 ms (trái với kỳ vọng thông thường). Độ trễ ở tải cố định được đo riêng ở thí nghiệm E4.
- Kafka chỉ có 1 broker, replication factor bằng 1, nên acks=all không có thêm bản sao nào để chờ, độ bền thực tế gần như acks=1. acks=0 chỉ đo thời gian đẩy dữ liệu ra, không chờ xác nhận từ Kafka, nên độ trễ của nó không so sánh trực tiếp với hai mức còn lại.

## E7. REST API (bản chạy lại bằng 127.0.0.1)

| Cấu hình | Request/giây | Sự kiện/giây | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|---|
| Từng sự kiện, 1 client | 16,3 | 16 | 61 | 66 | 69 |
| Từng sự kiện, 10 client | 165,1 | 165 | 59 | 69 | 76 |
| Từng sự kiện, 50 client | 763,1 | 763 | 59 | 78 | 89 |
| Batch 100, 1 client | 15,0 | 1.500 | 62 | 85 | 105 |
| Batch 100, 10 client | 119,1 | 11.913 | 74 | 121 | 142 |
| Batch 100, 50 client | 272,2 | 27.214 | 160 | 253 | 295 |

Nhận xét:
- Gửi từng sự kiện: thông lượng tăng gần tuyến tính theo số client (16 → 165 → 763 sự kiện/giây khi 1 → 10 → 50 client), độ trễ giữ ổn định quanh 59–60 ms vì mỗi request chờ Kafka xác nhận.
- Gửi theo batch 100 sự kiện cho thông lượng cao hơn khoảng 36 lần so với gửi từng sự kiện ở 50 client (27.214 so với 763 sự kiện/giây), đổi lại độ trễ mỗi request tăng (p50 từ 60 lên 160 ms).
- Ở chế độ batch, 10 lên 50 client chỉ tăng thông lượng khoảng 2,3 lần trong khi số client tăng 5 lần, nên đã gần giới hạn của máy thử nghiệm.
- Không có request nào bị lỗi (requests_failed = 0 ở cả 18 lần chạy).

### So sánh trước và sau khi đổi localhost thành 127.0.0.1

| Cấu hình | Sự kiện/giây (localhost) | Sự kiện/giây (127.0.0.1) | Thay đổi | p99 cũ (ms) | p99 mới (ms) |
|---|---|---|---|---|---|
| Từng sự kiện, 1 client | 15 | 16 | 9% | 68 | 69 |
| Từng sự kiện, 10 client | 150 | 165 | 10% | 73 | 76 |
| Từng sự kiện, 50 client | 518 | 763 | 47% | 146 | 89 |
| Batch 100, 1 client | 1.260 | 1.500 | 19% | 109 | 105 |
| Batch 100, 10 client | 9.570 | 11.913 | 24% | 168 | 142 |
| Batch 100, 50 client | 21.342 | 27.214 | 28% | 2106 | 295 |

Phát hiện kỹ thuật: bản chạy đầu dùng `localhost` cho p99 khoảng 2,1 giây ở cấu hình batch 50 client, rất đều ở cả ba lần chạy. Sau khi đổi sang `127.0.0.1`, p99 giảm còn khoảng 295 ms và thông lượng tăng từ 9% đến 47%. Điều này khớp với giả thuyết: trên Windows, `localhost` thử IPv6 trước, mà API chỉ nghe trên IPv4, nên kết nối đầu tiên của mỗi client mất khoảng 2 giây. Các lần chạy sau cũng ổn định hơn hẳn (dao động giữa các lần chạy chỉ khoảng 1–2%). Phần chênh lệch ở 50 client lớn hơn mức 10% mà riêng 2 giây đầu có thể giải thích, nên có thể còn yếu tố khác của `localhost` mà nhóm chưa xác định.

## Hạn chế chung
- Tất cả thành phần chạy trên một máy, nên số đo bị chi phối bởi CPU và Docker Desktop trên Windows, không phản ánh cụm Kafka nhiều máy.
- Chỉ có 1 broker và replication factor bằng 1.
- Client tải viết bằng Python cũng tiêu tốn CPU của máy.
- Chưa kiểm tra riêng việc `localhost` có ảnh hưởng tương tự đến E2 và E3 hay không; lần chạy đầu của cấu hình gốc ở E3 thấp hơn hẳn hai lần sau, có thể liên quan hiệu ứng khởi động.
