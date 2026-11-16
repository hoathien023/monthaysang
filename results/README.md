# Thư mục results

Kết quả thí nghiệm do TV1 chạy (E2, E3, E7). Mỗi lần chạy lưu 1 file chi tiết trong `runs/`, `benchmark/run_tv1.py` gộp thành CSV.

| File | Nội dung |
|---|---|
| `E2.csv` | Số producer 1, 2, 4 (9 dòng = 3 cấu hình × 3 lần) |
| `E3.csv` | Cấu hình Producer: batch.size, linger.ms, nén, acks (27 dòng = 9 cấu hình × 3 lần) |
| `E7.csv` | REST API, chạy lại bằng địa chỉ `127.0.0.1` (18 dòng = 6 cấu hình × 3 lần). **Bộ số chính thức.** |
| `E7_localhost.csv` | Lần chạy đầu bằng địa chỉ `localhost` (18 dòng). Giữ lại làm minh chứng cho phát hiện kỹ thuật: p99 khoảng 2,1 giây do cách Windows phân giải `localhost`. File chi tiết của lần này đã bị ghi đè bởi lần chạy lại (trùng tên), nên chỉ còn bản CSV. |
| `runs/E2_*`, `runs/E3_*`, `runs/E7_*` | File JSON chi tiết từng lần chạy (E7 là của lần chạy lại bằng `127.0.0.1`) |
| `runs/thu1`, `thu2`, `thu3`, `e7_single_c10`, `e7_batch_c10` | Các lần chạy thử bằng tay ban đầu |
| `runs/<run_id>_timeline.csv` | Số sự kiện gửi trong từng giây (có từ bản Producer mới; dùng chứng minh tốc độ và tính liên tục, và để vẽ biểu đồ burst) |

Phân tích và nhận xét: [`../docs/Ket_qua_thi_nghiem_TV1.md`](../docs/Ket_qua_thi_nghiem_TV1.md).
Cột `ack_*_ms` là độ trễ từ lúc gọi `produce()` đến lúc Kafka xác nhận (đã gồm thời gian chờ trong hàng đợi cục bộ), không phải độ trễ end-to-end (do thí nghiệm E4 đo).
