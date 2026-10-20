#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BÀI THỰC HÀNH 1 - CHƯƠNG TRÌNH 2: CONSUMER
Lắng nghe và nhận message từ queue 'iot_lab_queue' trong RabbitMQ.
"""

import sys
import time
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
QUEUE_NAME = "iot_lab_queue"


def parse_arguments():
    """Hỗ trợ cấu hình qua dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="BTH1 - Consumer: Nhan message tu queue 'iot_lab_queue'."
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
        print("Vui lòng kiểm tra lại:")
        print(f"  1. RabbitMQ Server đã khởi động tại '{args.host}:{args.port}' chưa?")
        print("  2. Nếu dùng broker trên lớp/cloud, hãy dùng tham số: --host <IP> hoặc --url <URL>\n")
        sys.exit(1)


def callback(ch, method, properties, body):
    """Hàm xử lý khi có message mới đến queue."""
    message = body.decode("utf-8")
    receive_time = datetime.now().strftime("%H:%M:%S")

    # In ra đúng theo mẫu định dạng của đề bài
    print("------------------------------------------------------------")
    print(f"Da nhan message: {message}")
    print(f"Thoi gian nhan: {receive_time}")
    print("------------------------------------------------------------")

    # Xác nhận đã xử lý message thành công (Acknowledgement)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    args = parse_arguments()

    print("=" * 60)
    print("      BTH1 - AMQP CONSUMER (LẮNG NGHE QUEUE)")
    print("=" * 60)
    print(f"[*] Đang kết nối tới broker: {args.host}:{args.port}...")
    connection = get_connection(args)
    channel = connection.channel()

    # Khai báo queue (đảm bảo queue tồn tại trước khi consume)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Đảm bảo phân phối tải công bằng (mỗi worker nhận 1 message tại một thời điểm)
    channel.basic_qos(prefetch_count=1)

    # Đăng ký hàm callback nhận message
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=False
    )

    print(f"[*] Đang lắng nghe trên queue '{QUEUE_NAME}'. Nhấn Ctrl+C để thoát.")
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
