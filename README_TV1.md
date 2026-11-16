# Đồ án Nhập môn dữ liệu lớn (841450): Sources → Ingestion

Thu thập dữ liệu hành vi người dùng (clickstream) và giao dịch của một website thương mại điện tử giả lập,
đưa vào Apache Kafka theo thời gian thực. Sản phẩm gồm bộ sinh dữ liệu, Producer, REST API, Kafka,
Consumer kiểm chứng (đủ, đúng, liên tục) và bộ thí nghiệm đo tốc độ.

| Thành viên | Họ tên | MSSV | Vai trò |
|---|---|---|---|
| TV1 | ........................ | ............ | Nguồn dữ liệu, Producer, REST API |
| TV2 | ........................ | ............ | Hạ tầng Kafka, Consumer kiểm chứng |
| TV3 | ........................ | ............ | Thực nghiệm, đo đạc, minh chứng |

> Phân công chi tiết: `docs/` (file kế hoạch). Phần nào do ai viết được ghi ở đầu mỗi mục. Các mục ghi "TV2 bổ sung" và "TV3 bổ sung" đang chờ hai bạn hoàn thành.

## 1. Quy trình và ranh giới

- **Quy trình chọn:** Sources → Ingestion.
- **Điểm bắt đầu:** website/app giả lập (generator, Producer, REST API) phát sinh sự kiện.
- **Điểm kết thúc:** sự kiện nằm trong các topic Kafka; Consumer kiểm chứng xác nhận đã nhận đúng, đủ, liên tục.
- **Giao diện với nhóm sau (Ingestion → Storage):** các topic Kafka và [data contract](docs/data_contract.md).
- **Đặc trưng Big Data thể hiện:** Velocity (hàng nghìn đến hàng chục nghìn sự kiện/giây), Volume (hàng chục triệu sự kiện trong các thí nghiệm),
  Variety (2 loại sự kiện, giao dịch có danh sách mặt hàng lồng nhau), Veracity (sự kiện lỗi được phát hiện và đưa vào DLQ).

```
Generator ► Producer (Python) ──┐
                                ├─► Apache Kafka ──► Consumer kiểm chứng ──► log / CSV
Client ► REST API (FastAPI) ────┘   (web.clickstream, web.transactions)
                                     └─► web.events.dlq (sự kiện lỗi)
```
Sơ đồ chính thức: `docs/diagrams/` (TV3 bổ sung).

## 2. Yêu cầu môi trường

Đã chạy thật trên: **Windows 11 (64-bit, AMD64), Python 3.13, Docker Desktop (WSL2)**.

| Thành phần | Yêu cầu |
|---|---|
| Hệ điều hành | Windows 10/11 64-bit (Mac/Linux dùng được, lệnh kích hoạt venv khác) |
| Python | 3.10 trở lên (đã chạy với 3.13) |
| Docker Desktop | Bản mới, bật WSL2 (Windows). Nếu báo "WSL needs updating" thì chạy `wsl --install --no-distribution --web-download` (PowerShell Admin) rồi khởi động lại máy |
| Git | Để lấy mã nguồn |
| RAM / ổ đĩa | RAM 8 GB trở lên (cấp cho Docker ít nhất 4 GB), trống 10 GB |
| Cổng cần rảnh | 29092 (Kafka, cho chương trình chạy trên máy), 8080 (Kafka UI), 8000 (REST API) |
| Thư viện Python | `requirements.txt` (đã ghim phiên bản) |
| Image Docker | `apache/kafka:3.8.0`, `provectuslabs/kafka-ui` (theo `docker-compose.yml` ở thư mục gốc) |

## 3. Cài đặt và chạy nhanh (Windows PowerShell, tại thư mục gốc của repo)

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned   # nếu PowerShell chặn script
.venv\Scripts\Activate.ps1                                         # đầu dòng phải có (.venv)
pip install -r requirements.txt

docker compose up -d                    # dùng docker-compose.yml ở thư mục gốc (của TV2)
# Đợi khoảng 20-30 giây cho Kafka khởi động xong, rồi:
python infra/create_topics.py          # phải in: [OK] 6/6 topic san sang
```
Chương trình chạy trên máy kết nối Kafka qua **`localhost:29092`** (cổng 9092 chỉ dùng bên trong Docker). Producer và API đã mặc định cổng này;
muốn đổi địa chỉ, dùng `--bootstrap` (Producer) hoặc biến môi trường `KAFKA_BOOTSTRAP_SERVERS`.
Kiểm tra phần không cần Kafka (phải in `... OK` ở cả 3 lệnh):
```powershell
python -m src.generator.selftest
python -m src.generator.schema_check
python -m src.api.selftest
```
Kafka UI: http://localhost:8080. Mac/Linux: thay lệnh kích hoạt bằng `source .venv/bin/activate`.

## 4. Dữ liệu đầu vào (TV1)

- **Nguồn:** do generator tự sinh, mô phỏng website thương mại điện tử (5.000 người dùng, 2.000 sản phẩm, phiên truy cập, giao dịch VND/USD).
  Nhóm không dùng dữ liệu thật; lý do là cần kiểm soát tốc độ, quy mô và tỉ lệ lỗi để làm thí nghiệm lặp lại được.
- **Cách thu thập:** hai đường vào Kafka: (1) Producer Python gửi trực tiếp, (2) REST API nhận HTTP từ client rồi gửi vào Kafka.
- **Định dạng:** JSON UTF-8. Hai loại sự kiện `clickstream` và `transaction`.
  Schema, ví dụ bản ghi, mã lỗi: [`docs/data_contract.md`](docs/data_contract.md), [`schemas/`](schemas/).
- **Quy mô:** sinh 1.000.000 sự kiện trong khoảng 18 giây. Riêng E2 và E3 đã gửi hơn 36 triệu sự kiện.
- **Tốc độ:** cấu hình được bằng `--rate`; gửi hết tốc độ đạt khoảng 36.000 đến 53.000 sự kiện/giây với 1 producer trên máy thử nghiệm (trừ cấu hình `batch.size` 16K).
- **Tái lập:** cùng `--seed` cho cùng nội dung sự kiện (trừ `ts_event` là giờ thật lúc sinh).

Ví dụ bản ghi hợp lệ (clickstream):
```json
{"event_id":"9a1de644-815e-46d1-bb8f-aa1837f8a88b","producer_id":"P01","seq":2,"event_type":"page_view",
 "user_id":"U000260","session_id":"S027824","device":"mobile","ts_event":"2026-11-14T12:20:45.160Z",
 "product_id":"SKU-1466","category":"toys","url":"/product/SKU-1466"}
```
Dữ liệu mẫu: [`data/sample/sample.jsonl`](data/sample/sample.jsonl) (1.000 sự kiện, tỉ lệ lỗi 5%, định dạng ở mục 6 của data contract).

Sinh lại dữ liệu:
```powershell
python -m src.generator.generator --n 1000 --seed 42 --bad-ratio 0.05 --out data/sample/sample.jsonl
python -m src.generator.generator --n 1000000 --seed 1 --out data/large/big.jsonl     # dữ liệu lớn (~390 MB, không đưa lên git)
```
Mỗi lần sinh có thêm file `<out>.summary.json` ghi số sự kiện và số lỗi đã tiêm theo loại.

## 5. Producer (TV1)

Mặc định gửi tới `localhost:29092`. Lệnh ví dụ:

```powershell
python -m src.producer.producer --n 100000                       # 1 producer, hết tốc độ, 100.000 sự kiện
python -m src.producer.producer --producers 2 --rate 2000 --duration 60 --bad-ratio 0.05
python -m src.producer.producer --topic bench.p3 --n 500000      # ép toàn bộ vào 1 topic (thí nghiệm E1)
```

| Tham số | Ý nghĩa |
|---|---|
| `--producers` | Số tiến trình producer chạy song song (mỗi tiến trình có `producer_id` riêng: P01, P02…) |
| `--rate` | **Tổng** tốc độ mục tiêu (sự kiện/giây) của tất cả producer; 0 = hết tốc độ |
| `--n` / `--duration` | Dừng sau `n` sự kiện **mỗi producer** / sau số giây chỉ định (đến điều kiện nào trước) |
| `--acks` | `0`, `1` hoặc `all` (mặc định `all`; idempotence tự bật khi `acks=all`) |
| `--linger-ms`, `--batch-size`, `--compression` | Cấu hình gom batch và nén (`none`, `lz4`, `zstd`, `snappy`, `gzip`) |
| `--bad-ratio` | Tỉ lệ sự kiện bị tiêm lỗi có chủ đích (để đối chiếu với DLQ) |
| `--topic` | Ép toàn bộ sự kiện vào một topic (thí nghiệm); khi đó chỉ gửi loại clickstream |
| `--burst-rate`, `--burst-every`, `--burst-seconds` | Mô phỏng giờ cao điểm (xem dưới) |
| `--run-id`, `--seed`, `--txn-ratio`, `--bootstrap` | Tên lần chạy, seed, tỉ lệ giao dịch (mặc định 0,1), địa chỉ Kafka (mặc định `localhost:29092`) |

**Mô phỏng giờ cao điểm (burst):** cứ mỗi `--burst-every` giây, trong `--burst-seconds` giây cuối của chu kỳ,
tốc độ tăng từ `--rate` lên `--burst-rate`. Ví dụ:
```powershell
python -m src.producer.producer --rate 1000 --burst-rate 5000 --burst-every 10 --burst-seconds 3 --duration 30 --run-id demo_burst
```

**Log chứng minh tốc độ và tính liên tục.** Mỗi lần chạy in tổng kết và ghi:
- `results/runs/<run_id>.json`: số gửi, số Kafka xác nhận, số lỗi gửi, số lỗi đã tiêm theo loại, `seq` cuối của từng producer,
  độ trễ xác nhận p50/p95/p99, tốc độ nhỏ nhất, trung bình, lớn nhất mỗi giây và số giây không gửi được sự kiện nào.
- `results/runs/<run_id>_timeline.csv`: số sự kiện gửi trong từng giây. Vẽ biểu đồ: `python -m benchmark.plot_timeline <run_id>` (ghi `evidence/<run_id>_timeline.png`).
- Chương trình thoát với mã 2 nếu có sự kiện chưa được Kafka xác nhận.

**Kết quả chạy burst thực tế** (`--rate 1000 --burst-rate 5000 --burst-every 10 --burst-seconds 3 --duration 30`):
gửi 66.000 sự kiện, Kafka xác nhận đủ 66.000, 0 lỗi gửi; tốc độ mỗi giây từ 1.000 đến 5.000, **không có giây nào bị trống**;
độ trễ xác nhận p50 37 ms, p95 91 ms, p99 130 ms. Con số 66.000 khớp tính toán (3 chu kỳ × (7 giây × 1.000 + 3 giây × 5.000)).
Minh chứng: `evidence/E8_burst_terminal.png`, `evidence/demo_burst_timeline.png`.

## 6. REST API (TV1)

Chạy (cửa sổ terminal riêng, giữ cho nó chạy; phải dùng 1 worker để `seq` không trùng):
```powershell
uvicorn src.api.app:app --port 8000
```
Swagger: http://127.0.0.1:8000/docs

API đọc địa chỉ Kafka từ biến môi trường `KAFKA_BOOTSTRAP_SERVERS` (mặc định `localhost:29092`).

| Endpoint | Chức năng |
|---|---|
| `POST /events` | Nhận 1 sự kiện (đối tượng JSON) |
| `POST /events/batch` | Nhận danh sách tối đa 1.000 sự kiện |
| `GET /health` | Kiểm tra kết nối Kafka |
| `GET /stats` | Số request, số sự kiện, tốc độ trung bình từ lúc API chạy |

- Trả **202** khi Kafka đã xác nhận nhận sự kiện (không phải chỉ khi bỏ vào hàng đợi); **503** khi Kafka lỗi hoặc không xác nhận trong 10 giây; **422** nếu danh sách rỗng; **413** nếu quá 1.000 sự kiện.
- Nếu client không gửi `producer_id`, server gán `producer_id=API01`, `seq` tăng dần, và điền `event_id`, `ts_event` nếu thiếu.
- Topic được chọn theo `event_type` (giao dịch vào `web.transactions`, còn lại vào `web.clickstream`).
- API **không loại sự kiện sai**: đưa hết vào Kafka, việc kiểm tra và chuyển vào DLQ là của Consumer.

Ba lệnh gọi mẫu (PowerShell):
```powershell
# 1) Kiểm tra sức khỏe
Invoke-RestMethod http://127.0.0.1:8000/health

# 2) Gửi 1 sự kiện
$body = '{"event_type":"page_view","user_id":"U000001","session_id":"S1","device":"mobile","url":"/home"}'
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/events -ContentType "application/json" -Body $body

# 3) Gửi batch
$batch = '[{"event_type":"click","user_id":"U000002","session_id":"S2","device":"desktop","url":"/home"},{"event_type":"search","user_id":"U000003","session_id":"S3","device":"tablet","url":"/search?q=ao"}]'
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/events/batch -ContentType "application/json" -Body $batch
```
Mac/Linux: `curl -X POST http://127.0.0.1:8000/events -H "Content-Type: application/json" -d '{"event_type":"page_view","user_id":"U000001","session_id":"S1","device":"mobile","url":"/home"}'`

Giả lập nhiều client gọi API (cửa sổ terminal khác):
```powershell
python -m src.api.load_client --clients 10 --duration 30 --mode single --run-id e7_single_c10
python -m src.api.load_client --clients 10 --duration 30 --mode batch --batch-size 100 --run-id e7_batch_c10
```

## 7. Thí nghiệm (TV1: E2, E3, E7)

Chạy tự động (mỗi cấu hình 3 lần, 20 giây mỗi lần, khoảng 25 phút cho cả ba; E7 cần API đang chạy):
```powershell
python -m benchmark.run_tv1 --only E2
python -m benchmark.run_tv1 --only E3
python -m benchmark.run_tv1 --only E7
```
Kết quả: `results/E2.csv`, `E3.csv`, `E7.csv` (mô tả các file: [`results/README.md`](results/README.md)).
Phân tích đầy đủ và các hạn chế: [`docs/Ket_qua_thi_nghiem_TV1.md`](docs/Ket_qua_thi_nghiem_TV1.md).

| Thí nghiệm | Kết quả chính (trung bình 3 lần) |
|---|---|
| E2: số producer | 1 → 38.201; 2 → 67.290; 4 → 79.154 sự kiện/giây (gần tuyến tính từ 1 lên 2, rồi bão hòa) |
| E3: cấu hình Producer | Phần lớn cấu hình cho 43.000 đến 53.000 sự kiện/giây, chênh lệch nằm trong khoảng dao động; `batch.size` 16K gây nghẽn (khoảng 12% sự kiện chưa được xác nhận khi hết thời gian chờ) |
| E7: REST API | Gửi từng sự kiện: 763 sự kiện/giây (50 client). Gửi batch 100: 27.214 sự kiện/giây (50 client), p95 khoảng 253 ms |

Lưu ý: tất cả chạy trên một máy, 1 broker Kafka, replication factor 1. Trên Windows phải dùng `127.0.0.1` thay `localhost` khi gọi API
(dùng `localhost` làm p99 của E7 tăng lên khoảng 2,1 giây; bản chạy đó giữ ở `results/E7_localhost.csv`).

Thí nghiệm do TV2 và TV3 phụ trách: E1, E5, E6 (TV2 bổ sung), E4 (TV3 bổ sung).

## 8. Output (TV2 bổ sung)

Các topic Kafka (xem [data contract](docs/data_contract.md)): `web.clickstream`, `web.transactions`, `web.events.dlq`.
Phần Consumer (cách chạy, file tóm tắt đầu ra, ví dụ bản ghi trong DLQ) do TV2 bổ sung ở đây.

## 9. Cấu trúc thư mục

| Thư mục | Nội dung | Phụ trách |
|---|---|---|
| `infra/`, `docker-compose.yml` | Hạ tầng Kafka và Kafka UI, tạo topic (`infra/create_topics.py`) | TV2 |
| `schemas/`, `docs/` | JSON Schema, data contract, kết quả thí nghiệm, ghi nhận AI, đóng góp | TV1 (và cả nhóm) |
| `common/` | `validate.py` (kiểm tra sự kiện, dùng chung); `metrics.py` (TV3 bổ sung) | TV1, TV3 |
| `src/generator/` | Sinh sự kiện, tiêm lỗi, `selftest.py`, `schema_check.py` | TV1 |
| `src/producer/` | Producer | TV1 |
| `src/api/` | REST API, client giả lập tải, `selftest.py` | TV1 |
| `src/consumer/` | Consumer kiểm chứng | TV2 |
| `benchmark/` | `run_tv1.py` (E2, E3, E7), `plot_timeline.py` (biểu đồ tốc độ theo giây); runner chung của TV3 bổ sung | TV1, TV3 |
| `data/sample/` | Dữ liệu mẫu | TV1 |
| `results/`, `evidence/` | Số liệu đo và minh chứng (có `README.md` mô tả) | Cả nhóm |

## 10. Xử lý sự cố thường gặp

| Hiện tượng | Cách xử lý |
|---|---|
| `failed to connect to the docker API` | Docker Desktop chưa chạy: mở Docker Desktop, đợi "Engine running" |
| Docker báo "WSL needs updating" | PowerShell Admin: `wsl --install --no-distribution --web-download`, khởi động lại máy |
| Khởi động lại máy xong không vào được `localhost:8080` | Container không tự chạy lại: `docker compose up -d` (tại thư mục gốc) |
| Nhiều dòng `POLLHUP` hoặc `Coordinator load in progress` khi vừa bật Kafka | Kafka chưa khởi động xong; đợi 20-30 giây rồi chạy lại. Không phải lỗi |
| `.venv\Scripts\Activate.ps1 cannot be loaded` | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` |
| `ModuleNotFoundError: confluent_kafka` | Chưa kích hoạt `.venv` hoặc chưa `pip install -r requirements.txt` trong `.venv` |
| Gọi API qua `localhost` chậm bất thường | Dùng `127.0.0.1` |
| Mất topic sau khi dọn dẹp | Không dùng `docker compose down -v` khi đang thí nghiệm. Chạy lại `python infra/create_topics.py` |
| Producer/API báo không kết nối được Kafka (`Connect to ... failed`) | Kiểm tra `docker ps` (Kafka phải Up/healthy) và dùng cổng `29092`, không phải `9092` |

## 11. Ghi nhận dùng AI và đóng góp

[`docs/ai_usage.md`](docs/ai_usage.md), [`docs/contributions.md`](docs/contributions.md).
