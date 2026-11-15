# Ghi nhận sử dụng AI và Tính trung thực học thuật (Mục 8)

Tài liệu này ghi nhận trung thực việc sử dụng công cụ AI hỗ trợ trong quá trình thực hiện đồ án **Sources → Ingestion**, đảm bảo cả 3 thành viên đều hiểu rõ và giải thích được toàn bộ mã nguồn, cấu hình và số liệu thực nghiệm.

| Thành viên | Công cụ AI sử dụng | Mục đích hỗ trợ | Phần nhóm đã tự kiểm tra, chạy thử và chỉnh sửa |
|---|---|---|---|
| **TV1** | Trợ lý AI tạo sinh | Gợi ý cấu trúc generator, FastAPI endpoints, JSON Schema và script `run_tv1.py`. | Tự chạy `selftest`, `schema_check`, `api.selftest`; phát hiện và xử lý lỗi phân giải `localhost` vs `127.0.0.1` trên Windows trong E7; trực tiếp chạy E2, E3, E7 và phân tích kết quả. |
| **TV2** | Trợ lý AI tạo sinh | Gợi ý cấu hình `docker-compose.yml` (KRaft, listeners), `create_topics.py` và unit tests cho Consumer. | Tự kiểm chứng kết nối host↔container qua cổng `29092`, viết 40 unit tests `test_verify_consumer.py`, soạn lập luận kỹ thuật trong `docs/infra.md`. |
| **TV3** | Trợ lý AI tạo sinh | Hỗ trợ thiết kế `common/metrics.py`, `benchmark/run_benchmark.py`, script vẽ biểu đồ `matplotlib` và sơ đồ kiến trúc. | Tự kiểm thử đơn vị `tests/test_metrics.py`, chạy thực nghiệm Baseline và E1/E4/E5/E6 trực tiếp trên Docker Kafka, đối chiếu số liệu thực tế và kiểm tra clean run. |

## Cam kết của nhóm
- Mọi quyết định kỹ thuật (chọn Kafka KRaft 1 broker, key `user_id`, phân vùng 6/3/1, giữ nguyên `acks=all`, cách đo end-to-end cùng máy, dùng `p95/p99`) đều có lập luận của nhóm và được chứng minh bằng log/CSV thực tế.
- Không sao chép số liệu ảo; toàn bộ số liệu trong `results/` và `evidence/` đều được sinh ra từ các lần chạy thực tế trên máy tính của thành viên nhóm.
