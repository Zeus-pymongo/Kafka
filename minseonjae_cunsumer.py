# crawl_consumer.py
import os, json
from kafka import KafkaConsumer

# ⬇️ 네 크롤러 파일명으로 바꾸세요 (예: minseonjae_producer.py)
from minseonjae_producer import worker_crawl, MARIADB_COLUMN, MARIADB_ADDRESS_COLUMN

KAFKA_BOOTSTRAP = '192.168.0.223:9092'
KAFKA_TOPIC = 'crawl.raw'
GROUP_ID = 'mongo-writer'     # 같은 그룹으로 3개 띄우면 파티션 분배
CLIENT_ID = os.getenv("CLIENT_ID") or None  # 구분용(선택)

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        client_id=CLIENT_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        # 크롤링은 오래 걸리므로 poll/세션 타임아웃을 넉넉히
        max_poll_records=1,
        max_poll_interval_ms=1800000,  # 30분
        session_timeout_ms=30000,
        request_timeout_ms=305000,
    )

    try:
        for msg in consumer:
            payload = msg.value or {}
            name = payload.get("original_name")
            addr = payload.get("address")

            if not name:
                print(f"[skip] p={msg.partition} off={msg.offset} (no name)")
                consumer.commit()
                continue

            # 크롤러가 기대하는 입력 형태로 변환
            info = {MARIADB_COLUMN: name, MARIADB_ADDRESS_COLUMN: addr}

            try:
                result = worker_crawl(info)  # ⬅️ 여기서 Selenium 실행 + Mongo 업서트
                status = (result or {}).get('status')
                print(f"[consume] p={msg.partition} off={msg.offset} name={name} status={status}")
                # 성공 시에만 커밋 → at-least-once 보장
                consumer.commit()
            except Exception as e:
                print(f"[ERROR] p={msg.partition} off={msg.offset} name={name} err={e}")
                # 실패 시 커밋하지 않음 → 재처리
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

