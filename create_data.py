import json
import time
import random
import string
import os
import argparse
import pandas as pd
import sys
from datetime import datetime

# Khai báo đường dẫn gốc để Python nhận diện thư mục src
sys.path.append(os.getcwd())
from faker import Faker
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Monitoring
# from src.monitoring.monitoring_raw_data import MonitoringRawData

# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER_NAME", "create_data")
OUTPUT_FILE = os.getenv("OUTPUT_FILE_NAME", "realtime_reviews.jsonl")

fake = Faker()
# raw_data_monitor = MonitoringRawData()

conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)

# ==========================================
# 🌟 TỪ ĐIỂN DỮ LIỆU CHUẨN (CHO DASHBOARD BI)
# ==========================================
CATEGORIES = {
    "Điện tử": ["Điện thoại", "Laptop", "Tai nghe", "Đồng hồ thông minh"],
    "Thời trang": ["Áo thun", "Quần Jeans", "Giày Sneaker", "Áo khoác"],
    "Gia dụng": ["Máy xay sinh tố", "Lò vi sóng", "Nồi chiên không dầu", "Máy hút bụi"],
    "Mỹ phẩm": ["Son môi", "Serum", "Nước hoa", "Sữa rửa mặt"]
}
BRANDS = ["Apple", "Samsung", "Sony", "Nike", "Adidas", "Philips", "Lock&Lock", "L'Oreal", "Dyson", "LG"]

CITIES = {
    "Miền Bắc": ["Hà Nội", "Hải Phòng", "Quảng Ninh"],
    "Miền Trung": ["Đà Nẵng", "Huế", "Nha Trang"],
    "Miền Nam": ["Hồ Chí Minh", "Cần Thơ", "Đồng Nai"]
}
STORE_TYPES = ["Cửa hàng Vật lý", "Web Online", "Mobile App"]

PAYMENT_METHODS = ["Tiền mặt (COD)", "Thẻ Tín Dụng", "Momo", "ZaloPay", "VNPay"]
PAYMENT_STATUSES = ["Thành công", "Thành công", "Thành công", "Đang xử lý", "Thất bại"] # Tăng tỷ lệ thành công

SHIPPING_METHODS = ["Hỏa tốc", "Tiêu chuẩn", "Tiết kiệm"]
CARRIERS = ["Giao Hàng Nhanh", "Viettel Post", "Shopee Express", "Ninja Van", "J&T Express"]
SHIPPING_STATUSES = ["Đã giao", "Đã giao", "Đang vận chuyển", "Hoàn hàng"]

# ==========================================
# 2. CÁC HÀM XỬ LÝ LÕI
# ==========================================
def setup_kafka_topic():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    existing_topics = admin_client.list_topics(timeout=10).topics
    
    if KAFKA_TOPIC not in existing_topics:
        print(f"⚙️ Đang khởi tạo Topic '{KAFKA_TOPIC}' (10 Partitions, 1 Ngày Retention)...")
        new_topic = NewTopic(
            topic=KAFKA_TOPIC,
            num_partitions=10,        
            replication_factor=1,     
            config={'retention.ms': '86400000'}  
        )
        admin_client.create_topics([new_topic])
        time.sleep(2) 
        print("✅ Tạo Topic thành công!")
    else:
        print(f"✅ Topic '{KAFKA_TOPIC}' đã tồn tại và sẵn sàng.")

def inject_chaos(record):
    if random.random() < 0.15:
        error_type = random.choice(["mat_reviewID", "mat_asin", "mat_overall"])
        if error_type == "mat_reviewID": record["reviewerID"] = None
        elif error_type == "mat_asin": record["asin"] = None
        elif error_type == "mat_overall": record["overall"] = None
    return record

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Lỗi gửi tin lên Kafka: {err}")

def generate_random_id(length, prefix=""):
    chars = string.ascii_uppercase + string.digits
    return prefix + ''.join(random.choice(chars) for _ in range(length))

def generate_review_data():
    """Sinh một bản ghi E-commerce toàn diện với Product, Sales, Store, Payment, Shipping"""
    
    # 1. Thông tin Review Cũ (Giữ nguyên cấu trúc)
    reviewer_name = fake.name() if random.random() > 0.1 else None
    total_vote = random.randint(0, 500)
    helpful_yes = random.randint(0, total_vote) 
    helpful_list = [helpful_yes, total_vote]

    if random.random() > 0.15:
        review_text = fake.paragraph(nb_sentences=random.randint(2, 5))
    else:
        review_text = ''.join(random.choice(string.ascii_letters + " ") for _ in range(random.randint(20, 100)))

    # Fix: Nâng thời gian lên năm 2023 - 2024 cho Dashboard nhìn hợp thời đại
    start_date = int(datetime(2023, 1, 1).timestamp())
    end_date = int(datetime(2024, 12, 31).timestamp())
    unix_review_time = random.randint(start_date, end_date)
    review_time = datetime.fromtimestamp(unix_review_time).strftime('%m %d, %Y')

    # 2. Thông tin Product & Giao dịch mới
    category = random.choice(list(CATEGORIES.keys()))
    sub_category = random.choice(CATEGORIES[category])
    brand = random.choice(BRANDS)
    unit_price = round(random.uniform(50000, 5000000), 0) # Giá từ 50k đến 5 triệu
    quantity = random.randint(1, 4)

    # 3. Thông tin Store & Địa lý
    region = random.choice(list(CITIES.keys()))
    city = random.choice(CITIES[region])

    return {
        # --- THÔNG TIN REVIEW GỐC ---
        "reviewerID": generate_random_id(14),
        "asin": generate_random_id(8, prefix="B0"),
        "reviewerName": reviewer_name,
        "helpful": helpful_list,
        "reviewText": review_text,
        "overall": float(random.randint(1, 5)),
        "summary": fake.sentence(nb_words=random.randint(2, 6))[:-1],
        "unixReviewTime": unix_review_time,
        "reviewTime": review_time,
        "day_diff": random.randint(4, 1064),
        "helpful_yes": helpful_yes,
        "total_vote": total_vote,

        # --- 📦 THÔNG TIN SẢN PHẨM & ĐƠN HÀNG (MỚI) ---
        "productName": f"{sub_category} {brand} {fake.word().capitalize()}",
        "category": category,
        "sub_category": sub_category,
        "brand": brand,
        "unitPrice": unit_price,
        
        "orderID": generate_random_id(10, prefix="ORD-"),
        "quantity": quantity,
        "totalAmount": unit_price * quantity,

        # --- 🏪 THÔNG TIN CỬA HÀNG (MỚI) ---
        "storeID": f"STR-{region[:3].upper()}-{random.randint(1, 10)}",
        "storeName": f"Store {city} {random.randint(1, 3)}",
        "city": city,
        "region": region,
        "storeType": random.choice(STORE_TYPES),

        # --- 💳 THÔNG TIN THANH TOÁN (MỚI) ---
        "paymentID": generate_random_id(10, prefix="PAY-"),
        "paymentMethod": random.choice(PAYMENT_METHODS),
        "paymentStatus": random.choice(PAYMENT_STATUSES),

        # --- 🚚 THÔNG TIN GIAO HÀNG (MỚI) ---
        "shippingID": generate_random_id(10, prefix="SHP-"),
        "shippingMethod": random.choice(SHIPPING_METHODS),
        "carrierName": random.choice(CARRIERS),
        "shippingStatus": random.choice(SHIPPING_STATUSES)
    }

def process_and_send_record(record, file_handler):
    start_time = time.time() 
    record = inject_chaos(record)
    json_payload = json.dumps(record, ensure_ascii=False)
    
    file_handler.write(json_payload + "\n")
    file_handler.flush()
    
    producer.produce(topic=KAFKA_TOPIC, value=json_payload.encode('utf-8'), callback=delivery_report)
    producer.poll(0)
    
    # raw_data_monitor.log_ingestion(record, start_time)

# ==========================================
# 3. LUỒNG THỰC THI (REALTIME / BATCH)
# ==========================================
def run_realtime(file_handler):
    print(f"🚀 Đang chạy REAL-TIME. Đang bắn dữ liệu vào topic '{KAFKA_TOPIC}'...")
    try:
        while True:
            record = generate_review_data()
            process_and_send_record(record, file_handler)
            print(f"[{time.strftime('%H:%M:%S')}] Đã sinh & gửi 1 bản ghi giao dịch E-commerce...")
            time.sleep(0.5) 
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Real-time.")
    finally:
        producer.flush()

def run_batch(file_path, file_handler):
    print(f"📦 Chế độ BATCH tạm thời không hỗ trợ format data mới. Vui lòng dùng Real-time.")
    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Công tắc luồng dữ liệu E-commerce")
    parser.add_argument('--mode', type=str, choices=['realtime', 'batch'], required=True)
    parser.add_argument('--file', type=str, default='../data_kaggle/e-commerce.csv')
    args = parser.parse_args()

    setup_kafka_topic()

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    file_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)

    with open(file_path, "a", encoding="utf-8") as f:
        if args.mode == 'realtime':
            run_realtime(f)
        elif args.mode == 'batch':
            run_batch(args.file, f)