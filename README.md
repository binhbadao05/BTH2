# Bài Thực Hành 2: Mô Phỏng Cảm Biến IoT Gửi Dữ Liệu Môi Trường

> **Sinh viên thực hiện**: Nguyễn Phú Bình | **MSV**: B23DCCN083 | **Lớp**: D23CNPM06

## 1. Mục Tiêu
- Mô phỏng thiết bị IoT gửi dữ liệu telemetry định kỳ (nhiệt độ, độ ẩm) qua AMQP broker.
- Đóng gói dữ liệu dạng JSON chuẩn trong truyền thông IoT.
- Viết chương trình giám sát (Monitoring Consumer) tự động phân tích dữ liệu và kích hoạt cảnh báo khi vượt ngưỡng:
  - **Nhiệt độ > 35°C**: `CANH BAO: Nhiet do cao`
  - **Độ ẩm < 40%**: `CANH BAO: Do am thap`

---

## 2. Cấu Trúc Thư Mục
```text
BTH2/
├── sensor_producer_bai2.py     # Cảm biến IoT mô phỏng gửi dữ liệu mỗi 3s
├── monitor_consumer_bai2.py    # Hệ thống giám sát, phân tích JSON & cảnh báo
└── README.md                   # Hướng dẫn chi tiết Bài 2
```

---

## 3. Cấu Hình Broker & Queue
- **Broker**: RabbitMQ
- **Host**: `localhost` (hoặc IP broker dùng chung trên lớp / CloudAMQP)
- **Port**: `5672`
- **Tài khoản**: `guest` / `guest`
- **Queue sử dụng**: `sensor_data_queue`

---

## 4. Định Dạng Gói Tin JSON Telemetry
```json
{
  "device_id": "sensor01",
  "temperature": 36.4,
  "humidity": 38.9,
  "timestamp": "2026-10-19 20:25:00"
}
```

---

## 5. Hướng Dẫn Chạy Chương Trình

### Bước 1: Khởi động Monitoring Consumer
Mở Terminal 1 và chạy:
```bash
python monitor_consumer_bai2.py
```
> Consumer sẽ liên tục chờ và nhận dữ liệu từ `sensor_data_queue`.

### Bước 2: Khởi động Sensor Producer
Mở Terminal 2 và chạy:
```bash
python sensor_producer_bai2.py
```

#### Tùy chọn mở rộng:
- Thay đổi ID cảm biến và chu kỳ gửi (ví dụ: `sensor02`, gửi mỗi 2 giây):
  ```bash
  python sensor_producer_bai2.py --device-id "sensor02" --interval 2.0
  ```
- Mô phỏng chạy nhiều cảm biến đồng thời: Có thể mở thêm Terminal 3 và chạy với `--device-id "sensor03"`.

---

## 6. Kết Quả Đạt Được

### Tại màn hình Sensor Producer:
```text
============================================================
      BTH2 - SENSOR PRODUCER (MÔ PHỎNG CẢM BIẾN IOT)
============================================================
[*] Thiết bị: sensor01 | Chu kỳ gửi: 3.0s
[*] Đang kết nối tới broker: localhost:5672...
[+] Sẵn sàng gửi dữ liệu vào queue 'sensor_data_queue'. Nhấn Ctrl+C để dừng.

[x] #1 Đã gửi telemetry: {"device_id": "sensor01", "temperature": 36.4, "humidity": 38.9, "timestamp": "2026-10-19 20:25:00"}
[x] #2 Đã gửi telemetry: {"device_id": "sensor01", "temperature": 28.2, "humidity": 65.4, "timestamp": "2026-10-19 20:25:03"}
```

### Tại màn hình Monitoring Consumer:
```text
============================================================
      BTH2 - MONITORING CONSUMER (GIÁM SÁT CẢM BIẾN)
============================================================
[*] Ngưỡng cảnh báo: Nhiệt độ > 35.0°C | Độ ẩm < 40.0%
[*] Đang kết nối tới broker: localhost:5672...
[*] Đang lắng nghe trên queue 'sensor_data_queue'. Nhấn Ctrl+C để thoát.

------------------------------------------------------------
Device: sensor01
Temperature: 36.4
Humidity: 38.9
Timestamp: 2026-10-19 20:25:00
CANH BAO: Nhiet do cao
CANH BAO: Do am thap
------------------------------------------------------------
Device: sensor01
Temperature: 28.2
Humidity: 65.4
Timestamp: 2026-10-19 20:25:03
Trang thai: Binh thuong (Normal)
------------------------------------------------------------
```
