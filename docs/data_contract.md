# Data contract v1.0

Hợp đồng dữ liệu giữa các thành viên (và giữa nhóm này với nhóm Ingestion → Storage ở giai đoạn sau).
Phụ trách: TV1. Mọi thay đổi sau khi chốt phải báo cả nhóm.

- Định nghĩa chính thức: [`schemas/clickstream.schema.json`](../schemas/clickstream.schema.json), [`schemas/transaction.schema.json`](../schemas/transaction.schema.json).
- Kiểm tra chạy thật (nhanh, dùng cho Consumer và API): [`common/validate.py`](../common/validate.py).
- Ba thứ trên phải khớp nhau; lệnh kiểm chứng: `python -m src.generator.schema_check`.

## 1. Topic

| Topic | Partition | Key | Nội dung |
|---|---|---|---|
| `web.clickstream` | 6 | `user_id` | Sự kiện hành vi: `page_view`, `search`, `click`, `add_to_cart` |
| `web.transactions` | 3 | `user_id` | Sự kiện giao dịch: `order_created`, `payment_success`, `payment_failed` |
| `web.events.dlq` | 1 | `event_id` nếu có | Sự kiện không hợp lệ kèm lý do lỗi (dead-letter queue) |
| `bench.p1`, `bench.p3`, `bench.p6` | 1, 3, 6 | `user_id` | Chỉ dùng cho thí nghiệm E1 và các thí nghiệm đo tải; dữ liệu có cấu trúc như `web.clickstream` |

- Định dạng: JSON, mã hóa UTF-8, mỗi message là một đối tượng JSON.
- Key = `user_id`: các sự kiện của cùng một người dùng luôn vào cùng một partition và giữ đúng thứ tự.
  Sự kiện lỗi thiếu `user_id` không có key (Kafka tự chia partition).

## 2. Sự kiện clickstream (`web.clickstream`)

| Trường | Kiểu | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_id` | string (UUID) | Có | Mã sự kiện duy nhất |
| `producer_id` | string | Có | Nguồn gửi: `P01`, `P02`… (Producer), `API01` (REST API gán), `C01`… (client giả lập tải) |
| `seq` | integer ≥ 1 | Có | Số thứ tự tăng dần, liên tục, tính riêng cho từng `producer_id`; dùng phát hiện thiếu/trùng |
| `event_type` | string | Có | `page_view`, `search`, `click` hoặc `add_to_cart` |
| `user_id` | string | Có | Ví dụ `U000260` |
| `session_id` | string | Có | Ví dụ `S027824` |
| `product_id` | string | Không | Ví dụ `SKU-1466`; không có khi `event_type` là `search` |
| `category` | string | Không | Ví dụ `toys` |
| `device` | string | Có | `mobile`, `desktop` hoặc `tablet` |
| `url` | string | Có | Đường dẫn trang, ví dụ `/product/SKU-1466` hoặc `/search?q=sach` |
| `ts_event` | string | Có | Thời điểm sự kiện được tạo, ISO 8601 UTC dạng `2026-11-14T12:20:45.160Z`; dùng để tính độ trễ |

Ví dụ hợp lệ (`page_view`):
```json
{"event_id":"9a1de644-815e-46d1-bb8f-aa1837f8a88b","producer_id":"P01","seq":2,"event_type":"page_view",
 "user_id":"U000260","session_id":"S027824","device":"mobile","ts_event":"2026-11-14T12:20:45.160Z",
 "product_id":"SKU-1466","category":"toys","url":"/product/SKU-1466"}
```
Ví dụ hợp lệ (`search`, không có `product_id`):
```json
{"event_id":"5af30553-5ec4-4e08-a9a3-b2e95d65a441","producer_id":"P01","seq":7,"event_type":"search",
 "user_id":"U003714","session_id":"S219684","device":"desktop","ts_event":"2026-11-14T12:20:45.160Z",
 "url":"/search?q=sach"}
```

## 3. Sự kiện giao dịch (`web.transactions`)

| Trường | Kiểu | Bắt buộc | Mô tả |
|---|---|---|---|
| `event_id`, `producer_id`, `seq`, `user_id`, `ts_event` | như mục 2 | Có | Cùng quy ước với clickstream |
| `event_type` | string | Có | `order_created`, `payment_success` hoặc `payment_failed` |
| `order_id` | string | Có | Dạng `O` + `producer_id` + 9 chữ số, ví dụ `OP01000000002` |
| `items` | array | Có | Ít nhất 1 phần tử; mỗi phần tử gồm `product_id` (string), `qty` (integer ≥ 1), `price` (number > 0) |
| `total_amount` | number > 0 | Có | Tổng tiền; **phải bằng tổng `qty × price`** (sai số tối đa 0,01) |
| `currency` | string | Có | `VND` hoặc `USD` |
| `payment_method` | string | Có | `card`, `wallet`, `cod` hoặc `bank_transfer` |

Ví dụ hợp lệ:
```json
{"event_id":"fa5d3100-11b7-4948-90e6-e6607c69dee1","producer_id":"P01","seq":16,"event_type":"payment_success",
 "user_id":"U004002","order_id":"OP01000000002",
 "items":[{"product_id":"SKU-0118","qty":2,"price":1030000},{"product_id":"SKU-1939","qty":1,"price":2373000},
          {"product_id":"SKU-1498","qty":1,"price":400000}],
 "total_amount":4833000.0,"currency":"VND","payment_method":"wallet","ts_event":"2026-11-14T12:20:45.160Z"}
```

## 4. Ràng buộc mà JSON Schema không biểu diễn được

- `total_amount` bằng tổng `qty × price` (kiểm tra trong `common/validate.py`, mã lỗi `total_mismatch`).
- `seq` liên tục theo từng `producer_id` (Consumer kiểm tra bằng cách đếm số thiếu và số trùng).

## 5. Sự kiện không hợp lệ và DLQ

Sự kiện không hợp lệ không bị bỏ im lặng: Consumer đưa vào `web.events.dlq` với dạng (TV2 triển khai):
```json
{"source_topic":"web.clickstream","error_reason":"missing_field:user_id","ts_failed":"2026-11-14T12:21:03.001Z",
 "payload":"<chuỗi JSON gốc của sự kiện>"}
```
`payload` là chuỗi gốc vì có thể không phải JSON hợp lệ (khi đó `error_reason` là `invalid_json`).

Mã lỗi do `common/validate.py` trả về:

| Mã lỗi | Ý nghĩa |
|---|---|
| `not_an_object` | Message không phải đối tượng JSON |
| `missing_field:<tên>` | Thiếu trường bắt buộc |
| `wrong_type:<tên>` | Sai kiểu dữ liệu (ví dụ `seq` là chuỗi) |
| `bad_timestamp` | `ts_event` không đúng định dạng ISO 8601 UTC |
| `unknown_event_type` | `event_type` ngoài danh sách cho phép |
| `invalid_device`, `invalid_currency`, `invalid_payment_method` | Giá trị ngoài danh sách cho phép |
| `empty_items`, `invalid_item`, `invalid_qty` | Lỗi trong danh sách `items` |
| `negative_amount` | `price` hoặc `total_amount` không dương |
| `total_mismatch` | `total_amount` khác tổng `qty × price` |

Generator tiêm 6 loại lỗi có chủ đích (kèm nhãn để đối chiếu với DLQ ở thí nghiệm E6): thiếu trường, sai kiểu, `event_type` lạ,
timestamp sai, số tiền âm (chỉ giao dịch), `items` rỗng (chỉ giao dịch).

## 6. Dữ liệu mẫu

`data/sample/sample.jsonl`: 1.000 sự kiện (seed 42, tỉ lệ lỗi 5%). Mỗi dòng:
```json
{"topic":"web.clickstream","key":"U004597","event":{...},"injected_error":null}
```
`injected_error` là nhãn loại lỗi đã tiêm (hoặc `null` nếu hợp lệ). Tóm tắt: `data/sample/sample.jsonl.summary.json`.

## 7. Xác nhận của nhóm

| Thành viên | Họ tên | Xác nhận data contract v1.0 | Ngày |
|---|---|---|---|
| TV1 | ........................ | ☐ | ........ |
| TV2 | ........................ | ☐ | ........ |
| TV3 | ........................ | ☐ | ........ |
