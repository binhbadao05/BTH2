# BÁO CÁO BUỔI THỰC HÀNH 2: LẬP TRÌNH PYTHON VỚI GIAO THỨC AMQP (RABBITMQ)

> **Môn học**: Internet of Things (IoT) / Lập trình Mạng IoT  
> **Sinh viên thực hiện**: Nguyễn Phú Bình  
> **Mã sinh viên**: B23DCCN083  
> **Lớp**: D23CNPM06  

---

## 1. Mục Tiêu Buổi Thực Hành
Buổi thực hành giúp sinh viên nắm vững nguyên lý hoạt động và triển khai giao thức **AMQP (Advanced Message Queuing Protocol)** trong các bài toán IoT:
1. Hiểu mô hình Message Broker (RabbitMQ) và cách ứng dụng Python kết nối thông qua thư viện `pika`.
2. Vận dụng mô hình hàng đợi cơ bản: `Producer` -> `Queue` -> `Consumer`.
3. Đóng gói và truyền tải dữ liệu đo lường môi trường (telemetry) dạng JSON định kỳ, phát hiện cảnh báo vượt ngưỡng.
4. Điều phối thông điệp phân tán với **Exchange** và **Routing Key** (Direct Exchange), phân luồng cảnh báo theo các mức độ nghiêm trọng (`info`, `warning`, `critical`).

---

## 2. Cấu Trúc Mã Nguồn BTH2

```text
BTH2/
├── requirements.txt            # Thư viện pika
├── README.md                   # Báo cáo chi tiết Buổi 2 AMQP
│
├── producer_bai1.py            # [Bài 1] Producer gửi thông tin sinh viên vào queue
├── consumer_bai1.py            # [Bài 1] Consumer nhận message và in thời gian
│
├── sensor_producer_bai2.py     # [Bài 2] Cảm biến gửi telemetry JSON mỗi 3 giây
├── monitor_consumer_bai2.py    # [Bài 2] Consumer giám sát, cảnh báo nhiệt độ/độ ẩm
│
├── alert_producer_bai3.py      # [Bài 3] Producer phát cảnh báo theo các severity
├── warning_consumer_bai3.py    # [Bài 3] Consumer chỉ nhận cảnh báo WARNING
├── critical_consumer_bai3.py   # [Bài 3] Consumer chỉ nhận cảnh báo CRITICAL
└── all_alerts_consumer_bai3.py # [Bài 3 - Mở rộng] Nhận tất cả cảnh báo & ghi file log
```

---

## 3. Môi Trường & Cài Đặt

- **Ngôn ngữ**: Python 3.10+
- **Thư viện**: `pika>=1.3.2`
- **Broker**: RabbitMQ Server (chạy local trên port `5672` hoặc Docker)

### Cài đặt thư viện:
```bash
pip install -r requirements.txt
```

### Khởi động nhanh RabbitMQ Broker (nếu dùng Docker):
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
*(Giao diện quản trị RabbitMQ: `http://localhost:15672`, user/pass: `guest`/`guest`)*.

---

## 4. Hướng Dẫn Chạy Từng Bài

### 🔹 BÀI 1: Gửi và Nhận Message Cơ Bản Qua Queue
- **Queue**: `iot_lab_queue`
- **Chương trình**:
  - `consumer_bai1.py`: Lắng nghe queue `iot_lab_queue`.
  - `producer_bai1.py`: Gửi thông tin sinh viên (Nguyễn Phú Bình - B23DCCN083).
- **Cách chạy**:
  ```bash
  # Terminal 1: Chạy Consumer
  python consumer_bai1.py

  # Terminal 2: Chạy Producer
  python producer_bai1.py
  ```

---

### 🔹 BÀI 2: Mô Phỏng Cảm Biến IoT Gửi Dữ Liệu Môi Trường
- **Queue**: `sensor_data_queue`
- **Chương trình**:
  - `sensor_producer_bai2.py`: Sinh ngẫu nhiên nhiệt độ và độ ẩm, gửi JSON định kỳ mỗi 3s.
  - `monitor_consumer_bai2.py`: Nhận telemetry, hiển thị và cảnh báo:
    - `CANH BAO: Nhiet do cao` khi nhiệt độ > 35°C
    - `CANH BAO: Do am thap` khi độ ẩm < 40%
- **Cách chạy**:
  ```bash
  # Terminal 1: Chạy Monitoring Consumer
  python monitor_consumer_bai2.py

  # Terminal 2: Chạy Cảm Biến IoT
  python sensor_producer_bai2.py
  ```

---

### 🔹 BÀI 3: Mô Phỏng Hệ Thống Điều Phối Cảnh Báo Với Exchange
- **Exchange**: `iot_alert_exchange` (loại `direct`)
- **Routing Keys**: `info`, `warning`, `critical`
- **Chương trình**:
  - `warning_consumer_bai3.py`: Nhận cảnh báo mức `warning`.
  - `critical_consumer_bai3.py`: Nhận cảnh báo mức `critical`.
  - `all_alerts_consumer_bai3.py`: Nhận tất cả cảnh báo và ghi log vào `alerts.log`.
  - `alert_producer_bai3.py`: Phát cảnh báo theo mức độ.
- **Cách chạy**:
  ```bash
  # Terminal 1: Warning Consumer
  python warning_consumer_bai3.py

  # Terminal 2: Critical Consumer
  python critical_consumer_bai3.py

  # Terminal 3: Phát cảnh báo kiểm thử
  python alert_producer_bai3.py
  ```

---

## 5. Kết Luận Đạt Được
- Hoàn thành đầy đủ 100% các yêu cầu từ Bài 1 đến Bài 3 của buổi thực hành AMQP.
- Code được chuẩn hóa có đầy đủ xử lý ngoại lệ, UTF-8 trên Windows console, và hỗ trợ tham số dòng lệnh tùy biến broker host/port.
