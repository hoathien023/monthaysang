# Mục lục minh chứng (phần TV1)

| File | Chứng minh điều gì | Thí nghiệm / mục đề |
|---|---|---|
| `E0_kafka_ui_topics.png` | Đã tạo đủ topic, số partition đúng data contract | Mục 5.7, hạ tầng |
| `E0_kafka_ui_clickstream_messages.png` | Sự kiện từ Producer nằm trong `web.clickstream`, key là `user_id`, dữ liệu rải nhiều partition | Mục 5.4 |
| `E0_producer_thu1.png` | Producer chạy thật: gửi 10.000 sự kiện, Kafka xác nhận đủ, có tốc độ sự kiện/giây | Sản phẩm tối thiểu: log tốc độ |
| `E0_producer_kiemtra.png` | Producer bản mới: gửi 10.000 sự kiện, Kafka xác nhận đủ, in tốc độ mỗi giây và độ trễ xác nhận | Sản phẩm tối thiểu: log tốc độ |
| `E8_burst_terminal.png` | Mô phỏng giờ cao điểm: tốc độ tăng từ 1.000 lên 5.000 sự kiện/giây, gửi 66.000 sự kiện, Kafka xác nhận đủ, không có giây nào bị trống | Velocity, tính liên tục |
| `demo_burst_timeline.png` | Biểu đồ số sự kiện gửi mỗi giây của lần chạy burst (các bậc thang 1.000 → 5.000, lặp 3 chu kỳ) | Velocity |
| `E7_swagger_health_200.png` | API kết nối được Kafka (`/health` trả 200) | E7 |
| `E7_swagger_post_events_202.png` | `POST /events` trả 202 (Kafka đã xác nhận) | E7 |
| `E7_kafka_ui_api_event.png` | Sự kiện gửi qua API nằm trong Kafka, `producer_id` là `API01` | E7 |

Số liệu đo (log dạng CSV/JSON) nằm ở `../results/`. Log từng giây của lần chạy burst: `../results/runs/demo_burst_timeline.csv`.

## Có thể chụp thêm (không bắt buộc)
- [ ] Cửa sổ terminal chạy xong `python -m benchmark.run_tv1` cho E2, E3, E7 (dòng "Xong...").
- [ ] Kafka UI sau thí nghiệm: topic `bench.p6` và số message.

Phần minh chứng của TV2 (E1, E5, E6) và TV3 (E4, sơ đồ) bổ sung thêm vào bảng này.
