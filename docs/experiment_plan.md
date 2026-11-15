# Kế hoạch thực nghiệm và chuẩn đo đạc (TV3)

> Tài liệu chuẩn hóa phương pháp đo đạc, quy ước tham số và định dạng kết quả cho toàn bộ bộ thí nghiệm E1–E7 và đường cơ sở (baseline) trong đồ án **Sources → Ingestion**.

---

## 1. Mục tiêu và nguyên tắc chung

1. **Chạy trên cùng một máy vật lý:** Tất cả thành phần (Generator, Producer, FastAPI, Docker Kafka, Consumer kiểm chứng) chạy trên cùng một máy Windows để dùng chung đồng hồ hệ thống khi tính độ trễ end-to-end.
2. **Đóng ứng dụng nặng khi đo:** Hạn chế tác vụ nền để giảm nhiễu CPU/I/O.
3. **Lặp lại tối thiểu 3 lần (N ≥ 3):** Mỗi cấu hình chạy ít nhất 3 lần độc lập; báo cáo **trung bình (mean)**, **độ lệch chuẩn (std)** và khoảng **min–max**.
4. **Dọn sạch hoặc cô lập dữ liệu giữa các lần đo:** Các topic benchmark (`bench.p1`, `bench.p3`, `bench.p6`) được recreate hoặc tách bằng `group.id` mới mỗi lần chạy Consumer để không đếm nhầm sự kiện cũ.

---

## 2. Định nghĩa các chỉ số đo lường (`common/metrics.py`)

### 2.1. Throughput (Thông lượng)
- **Công thức:** `throughput_eps = events / duration_s` (đơn vị: **sự kiện/giây** — `events/s` hay `eps`).
- Phân biệt rõ hai phía đo:
  - **Producer throughput:** Số sự kiện được Kafka xác nhận (`delivered`) chia cho thời gian gửi thực tế (`wall_seconds`).
  - **Consumer throughput:** Số sự kiện hợp lệ/tổng nhận được chia cho `active_duration_s` (từ sự kiện đầu tiên đến sự kiện cuối cùng, loại bỏ thời gian `idle_timeout` chờ hết luồng).

### 2.2. Độ trễ (Latency, đơn vị: `ms`)
- **Độ trễ end-to-end (E4):**
  $$\text{latency\_ms} = (t_{\text{consumer\_read}} - t_{\text{event}}) \times 1000$$
  trong đó $t_{\text{event}}$ là trường `ts_event` (ISO 8601 UTC) được sinh ngay tại thời điểm tạo sự kiện và $t_{\text{consumer\_read}}$ là thời điểm Consumer đọc được bản ghi từ Kafka.
- **Độ trễ xác nhận Producer (E2, E3):** thời gian từ lúc gọi `produce()` đến lúc nhận callback ack từ Kafka (đã bao gồm thời gian chờ trong hàng đợi cục bộ).
- **Bách phân vị (`p50`, `p95`, `p99`):** Tính bằng nội suy tuyến tinh trên tập mẫu đã sắp xếp (`common/metrics.py::percentiles`). Dùng `p95` và `p99` để quan sát **tail latency** (5% và 1% sự kiện chậm nhất), tránh bị giá trị trung bình che lấp hiện tượng nghẽn đuôi.

### 2.3. Độ tin cậy và tính liên tục (`missing`, `duplicates`)
- Mỗi Producer có `producer_id` riêng (`P01`, `P02`, ...) và phát sinh `seq` tăng dần từ 1.
- Consumer theo dõi bitmap/set `seq` theo từng `producer_id` để đếm:
  - `missing`: số `seq` bị thiếu trong khoảng `[1 .. max_seq]` quan sát được.
  - `duplicates`: số lần `seq` xuất hiện lại.

---

## 3. Chuẩn CSV kết quả dùng chung

Mọi file kết quả chuẩn do Benchmark Runner hoặc `export_experiment_csv.py` xuất ra tuân theo 11 cột:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `experiment` | string | Mã thí nghiệm (`E1` .. `E7`, `BASELINE`) |
| `run` | int | Số thứ tự lần lặp (`1, 2, 3, ...`) |
| `params` | JSON string | Tham số cấu hình của lần chạy |
| `events` | int | Tổng số sự kiện đo được |
| `duration_s` | float | Thời gian chạy hiệu dụng (giây) |
| `throughput_eps` | float | Thông lượng (sự kiện/giây) |
| `p50_ms` | float | Trung vị độ trễ (ms) |
| `p95_ms` | float | Bách phân vị thứ 95 độ trễ (ms) |
| `p99_ms` | float | Bách phân vị thứ 99 độ trễ (ms) |
| `missing` | int | Số `seq` bị thiếu |
| `duplicates` | int | Số `seq` bị trùng |

*(Ghi chú tích hợp: Các file CSV chi tiết của TV1 trong `results/E2.csv`, `results/E3.csv`, `results/E7.csv` được giữ nguyên cấu trúc gốc và chuẩn hóa sang bảng tổng hợp khi vẽ biểu đồ E1–E7).*

---

## 4. Ma trận 7 thí nghiệm (E1–E7) & Đường cơ sở

| Mã | Tên thí nghiệm | Tham số biến thiên | Số lần lặp | Người phụ trách |
|---|---|---|---|---|
| **Baseline** | `kafka-producer-perf-test` | Topic `bench.p6`, 500.000 bản ghi, payload ~380B | 1–3 | TV3 |
| **E1** | Ảnh hưởng số partition | `bench.p1` (1), `bench.p3` (3), `bench.p6` (6), 1 producer, 500.000 sự kiện | 3 | TV2 / TV3 chạy bổ sung |
| **E2** | Ảnh hưởng số producer | 1, 2, 4 producer song song vào topic 6 partition | 3 | TV1 (đã có CSV) |
| **E3** | Cấu hình Producer | Đổi từng biến: `batch.size` (16/64/256 KB), `linger.ms` (0/5/20), `compression` (none/lz4/zstd), `acks` (0/1/all) | 3 | TV1 (đã có CSV) |
| **E4** | Độ trễ end-to-end theo tải | Tốc độ cố định `500`, `2.000`, `5.000` eps và `max` (0 = tối đa) | 3 | **TV3** |
| **E5** | Tính liên tục khi sự cố | Gửi tốc độ cố định ~1.000 eps; giữa chừng `docker restart kafka` | 1–3 | TV2 / TV3 chạy bổ sung |
| **E6** | Phát hiện dữ liệu lỗi | Tiêm 5% sự kiện lỗi (`--bad-ratio 0.05`); đối chiếu `invalid` vs `DLQ` | 3 | TV2 / TV3 chạy bổ sung |
| **E7** | Gửi qua REST API | `1, 10, 50` client đồng thời; chế độ `single` và `batch` (100) | 3 | TV1 (đã có CSV) |

---

## 5. Hạn chế của phép đo

- **Môi trường 1 máy (Single-host):** Producer, Consumer, FastAPI và Docker Kafka dùng chung CPU, RAM và đĩa, nên khi tải cao sẽ cạnh tranh tài nguyên lẫn nhau.
- **1 Broker / Replication Factor = 1:** Không phản ánh độ trễ sao chép đa node (`ISR replication`) hoặc khả năng chịu lỗi cụm thực tế.
- **Độ trễ đồng hồ & Python GIL:** Dù cùng máy để tránh lệch đồng hồ giữa các máy, quá trình tạo JSON và `asyncio`/`multiprocessing` trong Python vẫn tạo độ trễ cục bộ ở phía client khi đạt ngưỡng bão hòa.
