# Bảng phân công và đóng góp thành viên (TV3 tổng hợp)

> **Lưu ý:** Thư mục tích hợp `tv3-experiments-evidence` hiện gộp mã nguồn bàn giao từ gói nén của TV1 (`monthaysang-tv1-producer-api`) và TV2 (`monthaysang-tv2-infra-consumer`) cùng toàn bộ phần thực nghiệm, đo đạc và minh chứng của TV3. Khi đưa vào repo Git chung của nhóm, dùng lệnh `git shortlog -sn --all` và `git log --stat` để cập nhật số commit thực tế vào cột cuối.

| Mã | Họ và tên | MSSV | Vai trò chính | Sản phẩm & hạng mục đã hoàn thành | Ghi chú kiểm chứng |
|---|---|---|---|---|---|
| **TV1** | *(Điền họ tên)* | *(Điền MSSV)* | Nguồn dữ liệu, Producer & REST API | `docs/data_contract.md`, `schemas/*.json`, `common/validate.py`, `src/generator/`, `src/producer/`, `src/api/`, `data/sample/`, `benchmark/run_tv1.py`, `benchmark/plot_timeline.py`, chạy và phân tích **E2, E3, E7**, ảnh minh chứng E0/E7/E8. | Đã chạy qua `selftest`, `schema_check`, `api.selftest` (100% PASS). |
| **TV2** | *(Điền họ tên)* | *(Điền MSSV)* | Hạ tầng Kafka & Consumer kiểm chứng | `docker-compose.yml` (KRaft + Kafka UI), `infra/create_topics.py`, `consumer/verify_consumer.py`, `consumer/test_verify_consumer.py` (40 unit tests), `consumer/compare_e6.py`, `result/export_experiment_csv.py`, `docs/infra.md`. | 40/40 unit tests PASS; Consumer chạy thực tế kiểm chứng E4, E5, E6. |
| **TV3** | *(Điền họ tên)* | *(Điền MSSV)* | Thực nghiệm, đo đạc & minh chứng | Tích hợp code TV1+TV2 giữ nguyên mã nguồn gốc; `docker-compose.override.yml`, `requirements.txt`, `common/metrics.py`, `tests/test_metrics.py`, `docs/experiment_plan.md`, `benchmark/run_benchmark.py` + `config.json`, chạy **Baseline + E1, E4, E5, E6**, `benchmark/generate_charts.py` + biểu đồ `charts/*.png`, sơ đồ `docs/diagrams/architecture.drawio` + `.png`, `docs/Ket_qua_tong_hop_E1_E7.md`, `evidence/INDEX.md`, `README.md` tổng & kiểm tra **clean run**. | 4/4 unit tests `metrics.py` PASS; chạy đầy đủ Baseline & E1–E7 trên cùng máy. |

## Lệnh đối chiếu Git log (dùng khi commit lên repo chung)
```powershell
git shortlog -sn --all
git log --oneline -n 30
```
