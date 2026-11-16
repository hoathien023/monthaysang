# Checklist Việc Còn Lại Của Nhóm Trước Khi Nộp (TV1 – TV2 – TV3)

> Bản tích hợp đầy đủ mã nguồn, thực nghiệm, biểu đồ, sơ đồ và khung báo cáo Word đã nằm gọn trong thư mục `tv3-experiments-evidence`. Dưới đây là những việc cuối cùng mỗi thành viên cần hoàn tất trước khi nộp bài và bảo vệ.

---

## 1. Việc chung cả 3 thành viên (TV1 + TV2 + TV3)
- [ ] **Điền Họ tên, MSSV** của cả 3 thành viên vào:
  - Trang bìa file báo cáo `docs/Bao_cao_Do_an_Big_Data.docx`
  - Bảng đóng góp trong `docs/contributions.md`
- [ ] **Duyệt lại báo cáo & xuất PDF:** Đọc kiểm tra nội dung trong `docs/Bao_cao_Do_an_Big_Data.docx` (căn lề, chèn Mục lục tự động trong Word) → **Save As → PDF** để nộp.
- [ ] **Đẩy GitHub & chốt đóng góp:** Thống nhất repo GitHub chung của nhóm, mỗi người commit phần mình phụ trách (hoặc push theo nhánh rồi merge vào `main`), sau đó chạy `git shortlog -sn --all` để điền số commit thực tế vào `docs/contributions.md`.
- [ ] **Tập dượt vấn đáp chéo:** Cả 3 người nắm được luồng chung `Generator / REST API → Kafka (KRaft) → Consumer kiểm chứng / DLQ` để không bị bỡ ngỡ khi giảng viên hỏi chéo.

---

## 2. Việc còn lại riêng từng thành viên

### TV1 (Nguồn dữ liệu, Producer & REST API)
- [ ] Đọc kiểm tra lại phần nội dung của TV1 trong `docs/Bao_cao_Do_an_Big_Data.docx` (Mục 3 Dữ liệu, Mục 5 Producer/API, Mục 6 kết quả E2, E3, E7).
- [ ] Ôn lại 3 câu hỏi vấn đáp của TV1:
  1. Vì sao chọn `key = user_id`? Điều gì xảy ra nếu bỏ key?
  2. `enable.idempotence=True` và `acks=all` giải quyết vấn đề gì? Đánh đổi là gì?
  3. Vì sao cần trường `seq`? Phát hiện thiếu/trùng bằng cách nào?

### TV2 (Hạ tầng Kafka & Consumer kiểm chứng)
- [ ] Đọc kiểm tra lại phần nội dung của TV2 trong `docs/Bao_cao_Do_an_Big_Data.docx` (Mục 4 cấu hình Kafka/topic, Mục 5 Consumer/DLQ, Mục 6 kết quả E1, E5, E6).
- [ ] Đọc review chéo phần benchmark & tổng hợp của TV3 (`benchmark/run_benchmark.py`, `docs/Ket_qua_tong_hop_E1_E7.md`) theo đúng quy tắc review trong kế hoạch.
- [ ] Ôn lại 3 câu hỏi vấn đáp của TV2:
  1. Partition là gì? Tăng partition thì thông lượng và thứ tự thay đổi thế nào?
  2. Vì sao chọn KRaft, 1 broker, `replication factor = 1`? Hạn chế là gì?
  3. Consumer lag là gì? Offset được lưu ở đâu?

### TV3 (Thực nghiệm, Đo đạc & Minh chứng)
- [ ] Hoàn thiện tên/MSSV vào báo cáo Word, tổng hợp bản PDF cuối cùng sau khi TV1 & TV2 duyệt.
- [ ] *(Tùy chọn)* Chụp thêm 1–2 ảnh màn hình Kafka UI (`http://localhost:8080`) cho `web.events.dlq` / `bench.p6` bỏ vào `evidence/` nếu muốn báo cáo thêm sinh động.
- [ ] Ôn lại 3 câu hỏi vấn đáp của TV3 (xem số liệu dẫn chứng tại `docs/Ket_qua_tong_hop_E1_E7.md`):
  1. Throughput và Latency khác nhau thế nào? Vì sao dùng `p95/p99` thay vì chỉ trung bình?
  2. Vì sao mỗi cấu hình phải chạy ≥ 3 lần? Những yếu tố nào làm sai lệch số đo?
  3. Kết quả thực nghiệm cho thấy điều gì về khả năng mở rộng và giới hạn hiện tại?
