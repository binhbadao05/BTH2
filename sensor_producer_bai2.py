#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BÀI THỰC HÀNH 2 - CHƯƠNG TRÌNH 1: SENSOR PRODUCER
Mô phỏng cảm biến IoT đo nhiệt độ, độ ẩm và gửi dữ liệu dạng JSON định kỳ qua RabbitMQ.
"""

import sys
import time
import json
import random
import argparse
from datetime import datetime
import pika

# Đảm bảo in tiếng Việt chuẩn trên Windows console
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ==============================================================================
# CẤU HÌNH MẶC ĐỊNH
# ==============================================================================
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5672
DEFAULT_USER = "guest"
DEFAULT_PASS = "guest"
QUEUE_NAME = "sensor_data_queue"
DEFAULT_DEVICE_ID = "sensor01"
DEFAULT_INTERVAL = 3.0  # Chu kỳ 3 giây theo yêu cầu đề bài


def parse_arguments():
    """Hỗ trợ cấu hình qua dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="BTH2 - Sensor Producer: Mo phong cam bien IoT gui telemetry JSON."
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help="Dia chi RabbitMQ broker (mac dinh: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Cong RabbitMQ broker (mac dinh: 5672)"
    )
    parser.add_argument(
        "--user", default=DEFAULT_USER, help="Username RabbitMQ (mac dinh: guest)"
    )
    parser.add_argument(
        "--password", default=DEFAULT_PASS, help="Password RabbitMQ (mac dinh: guest)"
    )
    parser.add_argument(
        "--url", default=None, help="AMQP URL (vi du: amqp://user:pass@host:5672/)"
    )
    parser.add_argument(
        "--device-id", default=DEFAULT_DEVICE_ID, help="Ma dinh danh cam bien (mac dinh: sensor01)"
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL, help="Chu ky gui tin (giay, mac dinh: 3)"
    )
    parser.add_argument(
        "--count", type=int, default=0, help="So luong mau tin gui (0 = chay lien tuc)"
    )
    return parser.parse_args()


def get_connection(args):
    """Tạo kết nối tới RabbitMQ broker."""
    try:
        if args.url:
            parameters = pika.URLParameters(args.url)
        else:
            credentials = pika.PlainCredentials(args.user, args.password)
            parameters = pika.ConnectionParameters(
                host=args.host,
                port=args.port,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=2
            )
        return pika.BlockingConnection(parameters)
    except pika.exceptions.AMQPConnectionError as err:
        print(f"\n[!] LỖI KẾT NỐI BROKER: {err}")
        print(f"Không thể kết nối tới '{args.host}:{args.port}'. Vui lòng bật RabbitMQ Server.\n")
        sys.exit(1)


def generate_sensor_data(device_id):
    """
    Sinh dữ liệu cảm biến ngẫu nhiên:
    - Nhiệt độ trong khoảng: 20.0°C đến 42.0°C (có xuất hiện > 35°C để kích hoạt cảnh báo)
    - Độ ẩm trong khoảng: 30.0% đến 80.0% (có xuất hiện < 40% để kích hoạt cảnh báo)
    - Bổ sung thời gian gửi (timestamp) theo gợi ý mở rộng
    """
    temperature = round(random.uniform(20.0, 42.0), 1)
    humidity = round(random.uniform(30.0, 80.0), 1)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = {
        "device_id": device_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": timestamp
    }
    return data


def main():
    args = parse_arguments()

    print("=" * 60)
    print("      BTH2 - SENSOR PRODUCER (MÔ PHỎNG CẢM BIẾN IOT)")
    print("=" * 60)
    print(f"[*] Thiết bị: {args.device_id} | Chu kỳ gửi: {args.interval}s")
    print(f"[*] Đang kết nối tới broker: {args.host}:{args.port}...")

    connection = get_connection(args)
    channel = connection.channel()

    # Khai báo queue 'sensor_data_queue'
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"[+] Sẵn sàng gửi dữ liệu vào queue '{QUEUE_NAME}'. Nhấn Ctrl+C để dừng.\n")

    sent_count = 0
    try:
        while True:
            sent_count += 1
            payload = generate_sensor_data(args.device_id)
            json_body = json.dumps(payload, ensure_ascii=False)

            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json_body.encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )

            print(f"[x] #{sent_count} Đã gửi telemetry: {json_body}")

            if args.count > 0 and sent_count >= args.count:
                print(f"\n[+] Đã gửi đủ số lượng {args.count} tin nhắn.")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[!] Đang dừng cảm biến (Ctrl+C)...")
    finally:
        connection.close()
        print("[*] Đã đóng kết nối RabbitMQ an toàn.\n")


if __name__ == "__main__":
    main()
