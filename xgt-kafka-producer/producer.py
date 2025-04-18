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
        SELECT meas_dtm,  tag_id, datatyp, tag_group, datavalue
        FROM "MEASURE".meas_data
        WHERE meas_dtm = %s AND datavalue > 0
        ORDER BY tag_id
    """, (latest_dtm,))
    return cursor.fetchall()

# 데이터 전처리(이상치) 적용 대상 tag_id 목록
OUTLIER_TAG_IDS = {
    'IN_DOIT301A', 'IN_DOIT301B', 'IN_DOIT302A', 'IN_DOIT302B', 'IN_DOIT302C', 'IN_DOIT302D',

    'NH3_310B_PV', 'NH3_310D_PV', 

    'TP_EDGEN1_PV', 'NO3_310B_PV', 'NH3_310A_PV', 'NO3_310A_PV', 'NH3_311A_PV', 'NO3_311A_PV',
    'NH3_312A_PV', 'NO3_312A_PV', 'TP_EDGEN2_PV', 'NO3_310D_PV', 'NH3_310C_PV', 'NO3_310C_PV',
    'NO3_311B_PV', 'NH3_312B_PV', 'NO3_312B_PV',

    'INFLOW_PUMP_M3_INVERTER_HZ', 'INFLOW_PUMP_M2_INVERTER_HZ',

    'FIT501A', 'FIT501B', 'FIT511A', 'FIT511B',

    'TOC_EDGEN1_PV', 'SS_EDGEN1_PV', 'TOC_EDGEN2_PV', 'SS_EDGEN2_PV', 

    'FIT_201A_PV', 'FIT_201B_PV',

    'HYUM_TANK_MLSS301_A1', 'HYUM_TANK_MLSS301_B1',

    'DIT_301A_PV', 'MLSSIT_312A_PV', 'FIT_620_PV', 'DIT_301B_PV', 'MLSSIT_312B_PV',

}

def send_to_kafka(batch):
    for row in batch:
        meas_dtm, tag_id, datatyp, tag_group, value = row

        # 이상치 대상 키 적용
        key = b'outlier' if tag_id in OUTLIER_TAG_IDS else b'normal'

        data = {
            'meas_dtm': meas_dtm.isoformat(),
            'tag_id': tag_id,
            'data_type': datatyp,
            'tag_group':tag_group,
            'data_value': float(value)
        }
        producer.send(TOPIC_NAME, key=key, value=data)
        print(f"[{datetime.now()}] Sent to Kafka | Key: {key.decode()} | Value: {data}")

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
