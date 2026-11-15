# Tổng hợp kết quả thực nghiệm Baseline và E1–E7 (TV3)

Tài liệu này tổng hợp toàn bộ kết quả đo đạc thực tế trên cùng một máy Windows 11 (AMD64, Python 3.13.2, Docker Desktop WSL2, 1 Kafka broker KRaft + Kafka UI), kết hợp số liệu TV1 đã chạy (**E2, E3, E7**) và số liệu TV3 chạy kiểm chứng thực tế (**Baseline, E1, E4, E5, E6**).

---

## 1. Bảng tổng hợp nhanh (Mean ± Std, N = 3 lần lặp trừ E5)

| Thí nghiệm | Cấu hình / Điểm đo tiêu biểu | Số lần | Chỉ số chính (TB ± Độ lệch) | Min – Max | Biểu đồ |
|---|---|---:|---|---|---|
| **Baseline** | `kafka-producer-perf-test` (6P, 380B) | 3 | `58.524,8 ± 31.409,8` eps | 23.772,7 – 84.889,6 | [BASELINE_comparison.png](../charts/BASELINE_comparison.png) |
| **E1** | `1 partition` (`bench.p1`) | 3 | `5.055,0 ± 8,4` eps | 5.045,4 – 5.060,9 | [E1_partitions.png](../charts/E1_partitions.png) |
| **E1** | `3 partition` (`bench.p3`) | 3 | `23.405,6 ± 4.413,9` eps | 20.610,1 – 28.494,1 | [E1_partitions.png](../charts/E1_partitions.png) |
| **E1** | `6 partition` (`bench.p6`) | 3 | `23.967,7 ± 1.010,5` eps | 23.089,4 – 25.072,1 | [E1_partitions.png](../charts/E1_partitions.png) |
| **E2** | `1 / 2 / 4 producer` (6P) | 3 | `38.201` → `67.290` → `79.154` eps | 35.564 – 94.156 | [E2_producers.png](../charts/E2_producers.png) |
| **E3** | Cấu hình Producer (`batch=16K` nghẽn) | 3 | `9.762` eps (16K) vs `43.403–53.200` eps | 8.686 – 54.306 | [E3_producer_configs.png](../charts/E3_producer_configs.png) |
| **E4** | Tải `500` / `2000` / `5000` / `max` eps | 3 | `p95 = 2256` / `314` / `200` / `2501` ms | 196,6 – 5.515,2 ms | [E4_latency_vs_load.png](../charts/E4_latency_vs_load.png) |
| **E5** | Restart Kafka giữa lúc chạy (`1000` eps) | 1 | `75.000 / 75.000` sự kiện (`gap = 30,43s`) | `missing = 0, dup = 0` | Log `E5_run.log` |
| **E6** | Tiêm 5% dữ liệu lỗi (`bad-ratio=0.05`) | 3 | `1.212 / 1.212` lỗi vào DLQ (100% khớp) | `missing = 0, dup = 0` | Log `E6_run.log` |
| **E7** | REST API `single` vs `batch=100` (50 client) | 3 | `763,1` vs `27.214,3` eps (`p95 = 253` ms) | 16,0 – 27.346,6 | [E7_rest_api.png](../charts/E7_rest_api.png) |

---

## 2. Phân tích từng thí nghiệm

### 2.1. Đường cơ sở (`kafka-producer-perf-test`)
- Công cụ chuẩn Java của Kafka đạt trung bình **58.525 records/giây** (đỉnh **84.890 records/giây** ở lần 1), cao hơn khoảng **1,5–2,4 lần** so với 1 Python Producer đơn lẻ nhưng thấp hơn mô hình **4 Python Producer song song** (`79.154` eps).
- Độ dao động giữa 3 lần chạy rất lớn (`23.773 – 84.890` eps) do chạy bên trong Docker Desktop WSL2 và chia sẻ đĩa/CPU với hệ điều hành host.
- Đây là mốc tham chiếu chứng minh Python Producer (`confluent-kafka` bọc thư viện C `librdkafka`) có hiệu năng tiệm cận công cụ gốc của Kafka khi chạy đa tiến trình.

### 2.2. E1 – Ảnh hưởng của số Partition (`bench.p1`, `bench.p3`, `bench.p6`)
- Khi tăng từ **1 lên 3 partition**, thông lượng trung bình tăng vọt **4,6 lần** (từ `5.055` lên `23.406` eps) và độ trễ xác nhận `ack p95` giảm mạnh từ **~31,3 giây** xuống còn **~0,70 giây**.
- Từ **3 lên 6 partition**, thông lượng gần như đi ngang (`23.406` → `23.968` eps) vì chỉ có **1 Producer** và **1 Broker** chạy trên cùng máy, nút thắt lúc này nằm ở CPU sinh JSON của 1 tiến trình Python và băng thông ghi đĩa của 1 broker duy nhất.
- Số partition nhiều giúp phân tán hàng đợi ghi và mở đường chạy nhiều consumer song song, nhưng cần tăng đồng thời số producer/broker mới tiếp tục mở rộng tuyến tính.

### 2.3. E2 – Ảnh hưởng của số Producer (tổng hợp từ TV1)
- Tăng từ **1 lên 2 Producer** giúp tổng thông lượng tăng **1,76 lần** (`38.201` → `67.290` eps), thể hiện khả năng mở rộng tốt nhờ dùng `multiprocessing` tránh Python GIL.
- Tăng từ **2 lên 4 Producer** chỉ tăng thêm **17,6%** (`79.154` eps) trong khi độ trễ `ack p95` tăng từ **78 ms lên 133 ms**, cho thấy hệ thống đã chạm điểm bão hòa CPU/I/O của máy đơn.
- Toàn bộ các lần chạy đều có `failed = 0`, chứng minh cơ chế retry và idempotence hoạt động ổn định dưới tải cao.

### 2.4. E3 – Ảnh hưởng cấu hình Producer (tổng hợp từ TV1)
- Cấu hình **`batch.size = 16 KB`** là điểm bất thường rõ rệt nhất: thông lượng tụt xuống `9.762` eps, thời gian chạy kéo dài gấp 4 lần (82 giây) và chỉ `87,7%` sự kiện kịp xác nhận do số lượng request quá nhỏ lẻ làm phình hàng đợi cục bộ.
- Các cấu hình còn lại (`batch=64K/256K`, `linger=0/5/20`, `lz4/zstd`) đều đạt `43.400 – 53.200` eps với `100%` xác nhận; nén `zstd` làm đuôi độ trễ `p95` tăng lên ~608 ms vì tốn thêm CPU nén trên cùng máy host.
- Vì chạy trên **1 broker (RF=1)** nên `acks=all` thực tế không phải chờ replica khác, độ bền tương đương `acks=1`.

### 2.5. E4 – Độ trễ End-to-End theo mức tải (TV3 trực tiếp đo)
- Ở mức tải **2.000 eps** và **5.000 eps**, độ trễ end-to-end rất ổn định: `p50 = 190 → 125 ms`, `p95 = 314 → 200 ms`, `p99 = 441 → 477 ms`, `missing = 0, duplicates = 0`.
- Tại mức **`500 eps`**, `p50 = 627 ms` và `p95 = 2.256 ms` cao hơn mức 2.000 eps do cơ chế đọc lô mặc định của Consumer (`consume(num_messages=500, timeout=1.0)` phải đợi đủ 500 msg hoặc hết 1 giây mới trả về, cộng thêm độ trễ join consumer group lần đầu ở lần 1).
- Khi đẩy lên **tải tối đa (`max`, ~15.945 eps end-to-end)**, `p95` nhảy vọt lên **2.501 ms** (đỉnh lần 3 tới **5.515 ms**) và `p99` đạt **3.091 ms** (đỉnh **6.547 ms**). Đây là bằng chứng trực quan lý giải vì sao phải dùng `p95/p99` thay cho trung bình: tail latency phình to gấp >10 lần khi hàng đợi bão hòa.

### 2.6. E5 – Tính liên tục khi gặp sự cố Broker (`docker restart kafka`)
- Trong kịch bản gửi liên tục `1.000` eps suốt 75 giây và thực hiện `docker restart kafka` tại giây thứ 25 (thời gian lệnh restart mất 4,82 giây), hệ thống ghi nhận khoảng gián đoạn tối đa ở phía Consumer là **30,43 giây** (bao gồm thời gian broker khởi động lại và consumer rebalance).
- Sau khi broker phục hồi, Consumer nhận lại đầy đủ **75.000 / 75.000** sự kiện đã gửi, **`missing = 0`** và **`duplicates = 0`**.
- Kết quả khẳng định cơ chế đệm hàng đợi + retry của Producer kết hợp `enable.idempotence=True` và commit offset sau xử lý của Consumer đảm bảo không mất/trùng dữ liệu khi broker khởi động lại ngắn hạn.

### 2.7. E6 – Phát hiện dữ liệu lỗi và Dead-Letter Queue (DLQ)
- Qua 3 lần lặp với tỷ lệ tiêm lỗi `--bad-ratio 0.05` (tổng 24.000 sự kiện/lần), Producer tiêm chính xác **1.212** sự kiện lỗi và Consumer phát hiện **1.212** sự kiện không hợp lệ, đẩy đủ **1.212** bản ghi sang `web.events.dlq` (`dlq_delivered = 1.212`, `failed = 0`).
- Kiểm tra đối chiếu `valid + invalid == received` đạt **True** ở 100% các lần chạy, `missing = 0`, `duplicates = 0`.
- Điều này chứng minh đặc trưng **Veracity**: dữ liệu lỗi không bị loại im lặng mà được phân loại, gắn lý do lỗi (`error_reason`) và cách ly vào DLQ để truy vết.

### 2.8. E7 – Gửi sự kiện qua REST API (tổng hợp từ TV1)
- Gửi từng sự kiện (`POST /events`) đạt tối đa **763,1 eps** ở 50 client vì mỗi HTTP request phải đợi Kafka xác nhận.
- Chuyển sang gửi theo lô 100 sự kiện (`POST /events/batch`) tăng thông lượng lên **27.214,3 eps** (gấp **~35,7 lần**) ở 50 client với `p95 = 253 ms` và `0` request lỗi.
- Trên Windows, sử dụng `127.0.0.1` thay cho `localhost` giúp loại bỏ độ trễ phân giải IPv6 trước khi fallback về IPv4 (~2 giây ở kết nối đầu, giảm p99 từ 2.106 ms xuống 295 ms).

---

## 3. Hạn chế của phép đo và hướng cải tiến
1. **Chạy đơn máy (Single-host):** Producer, Consumer, FastAPI và Docker Desktop chia sẻ cùng CPU/RAM/disk nên số liệu chịu nhiễu chéo khi tải cao (đặc biệt thấy rõ ở E2 4 producer, Baseline và E4 `max`).
2. **1 Broker, Replication Factor = 1:** Chưa đánh giá được chi phí sao chép mạng (ISR replication) hay khả năng failover tự động khi một broker chết hẳn.
3. **Consumer mặc định `batch_size = 500`, `timeout = 1.0s`:** Gây độ trễ gom lô nhân tạo ở mức tải thấp (500 eps trong E4). Nếu triển khai thực tế cần độ trễ thấp ở tải nhẹ, nên giảm kích thước lô đọc hoặc timeout chờ của Consumer.
