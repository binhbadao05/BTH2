#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BÀI THỰC HÀNH 2 - CHƯƠNG TRÌNH 2: MONITORING CONSUMER
Nhận dữ liệu từ 'sensor_data_queue', phân tích telemetry và phát cảnh báo môi trường.
"""

import sys
import json
import argparse
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

# Ngưỡng cảnh báo theo quy định của đề bài
TEMP_THRESHOLD = 35.0  # > 35: Cảnh báo nhiệt độ cao
HUMI_THRESHOLD = 40.0  # < 40: Cảnh báo độ ẩm thấp


def parse_arguments():
    """Hỗ trợ cấu hình qua dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="BTH2 - Monitoring Consumer: Giam sat du lieu cam bien va phat canh bao."
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
        print(f"Không thể kết nối tới '{args.host}:{args.port}'. Vui lòng kiểm tra RabbitMQ Server.\n")
        sys.exit(1)


def callback(ch, method, properties, body):
    """Xử lý phân tích dữ liệu JSON và phát hiện cảnh báo."""
    try:
        data = json.loads(body.decode("utf-8"))
        device_id = data.get("device_id", "Unknown")
        temperature = data.get("temperature", 0.0)
        humidity = data.get("humidity", 0.0)
        timestamp = data.get("timestamp", "")

        print("------------------------------------------------------------")
        print(f"Device: {device_id}")
        print(f"Temperature: {temperature}")
        print(f"Humidity: {humidity}")
        if timestamp:
            print(f"Timestamp: {timestamp}")

        # Kiểm tra điều kiện cảnh báo theo đề bài
        has_alert = False
        if temperature > TEMP_THRESHOLD:
            print("CANH BAO: Nhiet do cao")
            has_alert = True
        if humidity < HUMI_THRESHOLD:
            print("CANH BAO: Do am thap")
            has_alert = True

        if not has_alert:
            print("Trang thai: Binh thuong (Normal)")
        print("------------------------------------------------------------")

        # Xác nhận đã xử lý message thành công
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print(f"[!] Lỗi phân tích gói tin JSON: {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[!] Lỗi trong quá trình xử lý: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    args = parse_arguments()

    print("=" * 60)
    print("      BTH2 - MONITORING CONSUMER (GIÁM SÁT CẢM BIẾN)")
    print("=" * 60)
    print(f"[*] Ngưỡng cảnh báo: Nhiệt độ > {TEMP_THRESHOLD}°C | Độ ẩm < {HUMI_THRESHOLD}%")
    print(f"[*] Đang kết nối tới broker: {args.host}:{args.port}...")

    connection = get_connection(args)
    channel = connection.channel()

    # Khai báo queue 'sensor_data_queue'
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    # Đăng ký hàm callback nhận dữ liệu
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=False
    )

    print(f"[*] Đang lắng nghe trên queue '{QUEUE_NAME}'. Nhấn Ctrl+C để thoát.\n")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Đang dừng consumer theo yêu cầu người dùng (Ctrl+C)...")
        channel.stop_consuming()
    finally:
        connection.close()
        print("[*] Đã đóng kết nối RabbitMQ an toàn.\n")


if __name__ == "__main__":
    main()
