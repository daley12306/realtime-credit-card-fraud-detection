#!/usr/bin/env python3
"""
Sinh dữ liệu giao dịch thẻ tín dụng (VN-focused).
- Currency = VND
- VN chia 3 miền, mỗi miền 3-4 tỉnh/thành đại diện
- Mỗi user tối đa 2 card per bank; một user có thể có card của nhiều bank
- Mỗi card_number duy nhất (card -> owner mapping)
- Reuse rate card ~60-70% per user; user thực hiện tx với xác suất 50-60% (nếu không chọn user khác)
"""
import os
import json
import random
import time
import uuid
from datetime import datetime, timezone

# ------------------- Cấu hình -------------------
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "txs")
RATE_TX_PER_SEC = float(os.environ.get("RATE_TX_PER_SEC", 2.0))  # default 2 tx/s
USER_POOL_SIZE = int(os.environ.get("USER_POOL_SIZE", 300))

# ------------------- User Pool (VN only) -------------------
def build_user_pool(n=USER_POOL_SIZE):
    VN_PROVINCES = {
        "Bắc": ["Hà Nội", "Hải Phòng", "Bắc Ninh"],
        "Trung": ["Đà Nẵng", "Thừa Thiên Huế", "Quảng Nam"],
        "Nam": ["TP Hồ Chí Minh", "Bình Dương", "Cần Thơ"]
    }

    pool = []
    for i in range(n):
        region = random.choice(list(VN_PROVINCES.keys()))
        province = random.choice(VN_PROVINCES[region])
        pool.append({
            "user_id": f"U{i:04d}",
            "home_country": "VN",
            "home_region": region,
            "home_province": province,
            # credit_limit và avg_spend theo VND (phạm vi thực tế giả lập)
            "credit_limit": random.randint(10_000_000, 200_000_000),   # 10 triệu -> 200 triệu VND
            "avg_spend": random.uniform(200_000, 5_000_000),           # 200k -> 5 triệu VND
            "loyalty_points": random.randint(0, 5000)
        })
    return pool

# ------------------- Card BINs (6 số đầu) cho 4 ngân hàng (credit cards) -------------------
BANK_BINS = {
    "BIDV": ["970405"],
    "MB": ["404305"],
    "VIETCOMBANK": ["403368"],
    "AGRIBANK": ["970411"]
}

# Mappings to ensure uniqueness and per-user/card-bank constraints
card_to_user = {}       # card_number -> user_id
user_cards = {}         # user_id -> { bank_name: [card_numbers...] }

# Hàm tạo 1 card mới (6 chữ số BIN + 10 chữ số tail => 16 chữ số)
def create_new_card_for_bank(bank_name):
    bin6 = random.choice(BANK_BINS[bank_name])
    tail10 = f"{random.randint(0, 10**10 - 1):010d}"
    return bin6 + tail10  # 16 chữ số

def format_card_number(card_number):
    return ' '.join([card_number[i:i+4] for i in range(0, 16, 4)])

# ------------------- Provinces (3 miền, 3-4 tỉnh mỗi miền) -------------------
VN_PROVINCES = {
    "Bắc": ["Hà Nội", "Hải Phòng", "Bắc Ninh", "Quảng Ninh"],
    "Trung": ["Đà Nẵng", "Thừa Thiên Huế", "Quảng Nam", "Quảng Ngãi"],
    "Nam": ["TP Hồ Chí Minh", "Bình Dương", "Cần Thơ", "An Giang"]
}

# ------------------- IP & Geo helper (VN) -------------------
def random_public_ip():
    # sinh IP public đơn giản, tránh các dải private phổ biến
    while True:
        a = random.randint(1, 254)
        b = random.randint(0, 254)
        c = random.randint(0, 254)
        d = random.randint(1, 254)
        # exclude private/reserved
        if a == 10: continue
        if a == 127: continue
        if a == 169 and b == 254: continue
        if a == 192 and b == 168: continue
        if a == 172 and 16 <= b <= 31: continue
        return f"{a}.{b}.{c}.{d}"

def random_geo_in_vn():
    # lat range ~ 8 to 23, lon ~ 102 to 110 (approx Vietnam)
    lat = round(random.uniform(8.0, 23.0), 6)
    lon = round(random.uniform(102.0, 110.0), 6)
    return f"{lat},{lon}"

# ------------------- Card allocation helper -------------------
def ensure_user_has_card_for_bank(user_id, bank_name):
    """Đảm bảo user có tối đa 2 cards cho bank_name. Nếu chưa có, tạo (nếu đủ điều kiện)."""
    if user_id not in user_cards:
        user_cards[user_id] = {}
    bank_cards = user_cards[user_id].setdefault(bank_name, [])
    # if user already has <2 cards for this bank, create new one and assign
    if len(bank_cards) < 2:
        # tạo card đảm bảo chưa tồn tại
        for _ in range(10):
            card = create_new_card_for_bank(bank_name)
            if card not in card_to_user:
                bank_cards.append(card)
                card_to_user[card] = user_id
                return card
        # fallback: reuse existing if somehow couldn't create unique after attempts
        if bank_cards:
            return random.choice(bank_cards)
        else:
            # forced creation (rare)
            card = create_new_card_for_bank(bank_name)
            bank_cards.append(card)
            card_to_user[card] = user_id
            return card
    else:
        # already has 2 cards for this bank -> return random one
        return random.choice(bank_cards)

def get_or_create_card_for_user(user_id, reuse_prob=0.65):
    """
    Lấy 1 card cho user_id:
      - Nếu user có card(s), với xác suất reuse_prob sẽ reuse 1 trong các thẻ của họ.
      - Ngược lại, tạo 1 thẻ mới cho 1 bank (có thể là bank mới) nhưng tuân thủ max 2 cards/bank.
    Trả về (card_number, bank_name).
    """
    # lấy danh sách tất cả card của user
    user_all_cards = []
    if user_id in user_cards:
        for b, cards in user_cards[user_id].items():
            for c in cards:
                user_all_cards.append((c, b))

    if user_all_cards and random.random() < reuse_prob:
        # reuse một card hiện có
        card, bank = random.choice(user_all_cards)
        return card, bank

    # tạo mới: chọn bank ngẫu nhiên (có thể user đã có thẻ của bank đó)
    bank = random.choice(list(BANK_BINS.keys()))
    card = ensure_user_has_card_for_bank(user_id, bank)
    return card, bank

# ------------------- Sinh giao dịch -------------------
def generate_transaction(user_pool,
                         user_active_prob_range=(0.50, 0.60),
                         card_reuse_p_range=(0.60, 0.70),
                         diff_location_prob=0.08,
                         online_weight=0.6):
    MERCHANTS_BY_CATEGORY = {
        "electronics": {
            "online": ["Shopee", "Lazada", "Tiki", "FPTShopOnline"],
            "instore": ["FPTShop", "NguyenKim", "TheGioiDiDong"]
        },
        "groceries": {
            "online": ["VinMartOnline", "BachHoaXanhOnline", "GrabFood"],
            "instore": ["Co.opMart", "BigC", "VinMart"]
        },
        "fashion": {
            "online": ["Shopee", "Lazada", "Tiki"],
            "instore": ["SixDotore", "LVstore", "ZaraVN"]
        },
        "travel": {
            "online": ["VietjetBooking", "VNPayTravel", "Traveloka"],
            "instore": ["VietnamAirlinesDesk", "SaigonTourDesk", "LocalTravelAgency"]
        },
        "entertainment": {
            "online": ["NetflixVN", "ZingMP3", "FPTPlay"],
            "instore": ["CGV", "GalaxyCinema", "GameCenter"]
        },
        "health": {
            "online": ["NhathuocOnline", "MediCareOnline"],
            "instore": ["Pharmacity", "AnKhangClinic", "FamilyMedical"]
        }
    }

    ALL_MERCHANTS = list({m for v in MERCHANTS_BY_CATEGORY.values() for t in v.values() for m in t})

    user = random.choice(user_pool)
    if random.random() >= random.uniform(*user_active_prob_range):
        user = random.choice(user_pool)

    user_id = user["user_id"]
    card_number, card_bank = get_or_create_card_for_user(user_id, reuse_prob=random.uniform(*card_reuse_p_range))

    if random.random() < (1 - diff_location_prob):
        merchant_region, merchant_province = user["home_region"], user["home_province"]
    else:
        merchant_region = random.choice(list(VN_PROVINCES.keys()))
        merchant_province = random.choice(VN_PROVINCES[merchant_region])

    merchant_country = "VN"
    currency = "VND"

    raw_amount = random.expovariate(1.0 / max(1.0, user["avg_spend"]))
    amount = round(raw_amount * random.uniform(0.5, 3.0), 0)
    if amount > user["credit_limit"]:
        amount = int(user["credit_limit"] * random.uniform(0.05, 1.0))

    transaction_type = random.choices(["online_purchase", "in_store"], weights=[online_weight, 1 - online_weight])[0]
    merchant_category = random.choice(list(MERCHANTS_BY_CATEGORY.keys()))

    typ_key = "online" if transaction_type == "online_purchase" else "instore"
    options = MERCHANTS_BY_CATEGORY[merchant_category][typ_key]
    merchant = random.choice(options)

    device_type = "web" if transaction_type == "online_purchase" else "pos"

    tx = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "user_id": user_id,
        "user_home_region": user["home_region"],
        "user_home_province": user["home_province"],
        "card_number": card_number,
        "card_bank": card_bank,
        "merchant": merchant,
        "merchant_country": merchant_country,
        "merchant_region": merchant_region,
        "merchant_province": merchant_province,
        "merchant_category": merchant_category,
        "transaction_type": transaction_type,
        "amount": amount,
        "currency": currency,
        "device_type": device_type,
        "device_ip": random_public_ip(),
        "geo_location": random_geo_in_vn(),
        "loyalty_points": user["loyalty_points"],
        "payment_method": "credit_card",
        "transaction_status": random.choices(["approved", "pending", "declined"], weights=[0.5, 0.05, 0.45])[0]
    }

    return tx


# ------------------- Main loop (kafka optional) -------------------
def main():
    user_pool = build_user_pool()

    # Kafka 
    producer = None
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=0
        )
        print(f"Connected to Kafka brokers: {KAFKA_BROKERS}, topic: {KAFKA_TOPIC}")
    except Exception as e:
        print(f"Kafka not available ({e}), dữ liệu vẫn in ra terminal thôi.")

    lambda_rate = max(RATE_TX_PER_SEC, 1e-9)

    try:
        while True:
            tx = generate_transaction(user_pool)
            line = json.dumps(tx, ensure_ascii=False)
            # In ra terminal (JSON) — nếu muốn pretty print, mình có hàm khác
            print(line, flush=True)

            # Gửi Kafka nếu có producer
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, tx)
                except Exception as e:
                    print(f"Lỗi gửi Kafka: {e}")

            # Sleep theo phân bố Poisson/exponential
            sleep_sec = random.expovariate(lambda_rate)
            time.sleep(sleep_sec)

    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        if producer:
            try:
                producer.flush()
                producer.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
