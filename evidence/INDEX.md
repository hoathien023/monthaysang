# Mục lục Minh chứng (Evidence Index)

Thư mục `evidence/` lưu toàn bộ ảnh chụp màn hình, biểu đồ timeline và file log thu thập trực tiếp trong quá trình triển khai và chạy thực nghiệm của cả nhóm.

| Mã / Giai đoạn | Tên file | Loại minh chứng | Nội dung chứng minh |
|---|---|---|---|
| **E0 (Cài đặt & Kết nối)** | `E0_docker_ps.log` | Log terminal | Xác nhận container `kafka` (Healthy) và `kafka-ui` đang chạy qua `docker compose ps`. |
| **E0** | `E0_kafka_ui_topics.png` | Ảnh Kafka UI | Danh sách các topic đã tạo trên Kafka UI (`:8080`). |
| **E0** | `E0_kafka_ui_clickstream_messages.png` | Ảnh Kafka UI | Bản ghi JSON thực tế trong `web.clickstream` kèm key `user_id`. |
| **E0** | `E0_producer_kiemtra.png`, `E0_producer_thu1.png` | Ảnh terminal | Producer gửi thử thành công và xác nhận không lỗi. |
| **Baseline** | `BASELINE_perf_test.log` | Log terminal | Log chạy `kafka-producer-perf-test.sh` (3 lần × 200.000 bản ghi) làm đường cơ sở. |
| **E1 (Partitions)** | `E1_topics_describe.log` | Log Kafka CLI | Mô tả chi tiết partition & ISR của tất cả 6 topic (`web.*`, `bench.*`). |
| **E1** | `E1_run.log` | Log benchmark | Log đầy đủ 9 lần chạy E1 (`bench.p1`, `bench.p3`, `bench.p6` × 3 lần). |
| **E4 (Latency vs Load)** | `E4_run.log` | Log benchmark | Log chạy Producer + Consumer đo độ trễ end-to-end ở 4 mức tải (`500`, `2000`, `5000`, `max`). |
| **E5 (Continuity)** | `E5_run.log` | Log benchmark | Log chứng minh gửi 75.000 sự kiện liên tục khi `docker restart kafka` ở giây 25 (`missing=0, dup=0`). |
| **E6 (Data Quality / DLQ)** | `E6_run.log` | Log benchmark | Log đối chiếu 3 lần chạy tiêm 5% lỗi: `injected=1212 == invalid=1212 == dlq_delivered=1212`. |
| **E6** | `E6_dlq_sample.log` | Log Kafka CLI | 5 bản ghi lỗi mẫu đọc trực tiếp từ topic `web.events.dlq`. |
| **E7 (REST API)** | `E7_swagger_health_200.png` | Ảnh Swagger | Giao diện Swagger `/docs` trả `200 OK` cho `GET /health`. |
| **E7** | `E7_swagger_post_events_202.png` | Ảnh Swagger | `POST /events` trả `202 Accepted` khi Kafka xác nhận. |
| **E7** | `E7_kafka_ui_api_event.png` | Ảnh Kafka UI | Sự kiện gửi qua REST API xuất hiện trên topic Kafka. |
| **E8 (Burst)** | `E8_burst_terminal.png`, `demo_burst_timeline.png` | Ảnh + Biểu đồ | Chứng minh mô phỏng giờ cao điểm (`1.000 → 5.000` eps), không có giây trống. |
| **Mô tả TV1** | `README.md` | Tài liệu | Ghi chú chi tiết của TV1 cho các ảnh E0, E7, E8. |
