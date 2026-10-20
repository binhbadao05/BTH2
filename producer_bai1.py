#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BÀI THỰC HÀNH 1 - CHƯƠNG TRÌNH 1: PRODUCER
Gửi message cơ bản qua queue 'iot_lab_queue' trong RabbitMQ.
"""

import sys
import time
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
# CẤU HÌNH MẶC ĐỊNH (Sinh viên có thể sửa trực tiếp hoặc truyền qua command-line)
# ==============================================================================
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5672
DEFAULT_USER = "guest"
DEFAULT_PASS = "guest"
QUEUE_NAME = "iot_lab_queue"

DEFAULT_STUDENT_NAME = "Nguyen Phu Binh"
DEFAULT_STUDENT_ID = "B23DCCN083"


def parse_arguments():
    """Hỗ trợ truyền các tham số tùy chọn qua dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="BTH1 - Producer: Gui message thong tin sinh vien vao RabbitMQ queue."
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
        "--url", default=None, help="AMQP URL (vi du: amqp://user:pass@host:5672/ hoac CloudAMQP URL)"
    )
    parser.add_argument(
        "--name", default=DEFAULT_STUDENT_NAME, help="Ho va ten sinh vien"
    )
    parser.add_argument(
        "--id", dest="student_id", default=DEFAULT_STUDENT_ID, help="Ma sinh vien"
    )
    parser.add_argument(
        "--count", type=int, default=1, help="So luong message muon gui (mac dinh: 1)"
    )
    parser.add_argument(
        "--interval", type=float, default=1.0, help="Khoang nghi giua cac lan gui (giay)"
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
        print("  2. Tài khoản/mật khẩu đăng nhập có chính xác không?")
        print("  3. Nếu dùng broker trên lớp/cloud, hãy dùng tham số: --host <IP> hoặc --url <URL>\n")
        sys.exit(1)


def main():
    args = parse_arguments()

    print("=" * 60)
    print("      BTH1 - AMQP PRODUCER (GỬI MESSAGE VÀO QUEUE)")
    print("=" * 60)
    print(f"[*] Đang kết nối tới broker: {args.host}:{args.port}...")
    connection = get_connection(args)
    channel = connection.channel()

    # Khai báo queue 'iot_lab_queue' (đảm bảo queue tồn tại)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"[+] Đã kết nối thành công. Queue: '{QUEUE_NAME}'")

    try:
        for i in range(1, args.count + 1):
            # Nội dung message theo yêu cầu đề bài
            message_body = (
                f"Xin chao tu ung dung Python AMQP - {args.student_id} - {args.name}"
            )
            if args.count > 1:
                message_body += f" (Message #{i})"

            # Gửi message vào queue qua Default Exchange
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=message_body.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # Lưu message bền vững
                )
            )
            print(f"[x] Đã gửi [{i}/{args.count}]: \"{message_body}\"")

            if i < args.count:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[!] Đã dừng gửi do người dùng ngắt chương trình (Ctrl+C).")
    finally:
        connection.close()
        print("[*] Đã đóng kết nối RabbitMQ an toàn.\n")


if __name__ == "__main__":
    main()
