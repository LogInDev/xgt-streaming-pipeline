import os
import time
import json
import psycopg2
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# 환경 변수 로딩
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'port': int(os.getenv("DB_PORT")),
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
}

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

# DB 연결
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# 중복 방지용 최신 meas_dtm 저장
last_sent_dtm = None

def get_next_batch():
    global last_sent_dtm

    # 가장 최신 시간 가져오기
    cursor.execute("""
        SELECT meas_dtm
        FROM "MEASURE".meas_data
        ORDER BY meas_dtm DESC
        LIMIT 1
    """)
    result = cursor.fetchone()
    if not result:
        return []

    latest_dtm = result[0]
    if latest_dtm == last_sent_dtm:
        print(f"[{datetime.now()}] No new data. Skipping.")
        return []

    last_sent_dtm = latest_dtm

    # 해당 시간 기준 데이터 조회
    cursor.execute("""
        SELECT meas_dtm, tag_id, datavalue
        FROM "MEASURE".meas_data
        WHERE meas_dtm = %s AND datavalue > 0
        ORDER BY tag_id
        LIMIT 300
    """, (latest_dtm,))
    return cursor.fetchall()

def send_to_kafka(batch):
    for row in batch:
        meas_dtm, tag_id, value = row
        data = {
            'timestamp': meas_dtm.isoformat(),
            'sensor_name': tag_id,
            'value': float(value)
        }
        producer.send(TOPIC_NAME, data)
        print(f"[{datetime.now()}] Sent: {data}")

if __name__ == "__main__":
    print(f"[{datetime.now()}] Producer started. Target topic: {TOPIC_NAME}")
    try:
        while True:
            rows = get_next_batch()
            if rows:
                send_to_kafka(rows)
            time.sleep(60)
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: {e}")
    finally:
        cursor.close()
        conn.close()
        producer.close()
