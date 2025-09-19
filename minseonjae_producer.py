# producer_min.py  — MariaDB → Kafka (작업만 적재)
import json, hashlib
import pandas as pd
import pymysql
from kafka import KafkaProducer

# --- 설정 ---
MARIADB = {'host':'192.168.0.221','port':3306,'user':'jongro','password':'pass123#','db':'jongro','charset':'utf8'}
TABLE, COL_NAME, COL_ADDR, COL_STATUS = 'RESTAURANTS_GENERAL','STORE_NAME','DETAIL_ADD','OP_STATUS'
BOOTSTRAP = '192.168.0.223:9092'
TOPIC = 'crawl.raw'   # 필요시 topic_test 등으로 교체

def kkey(s:str)->bytes:
    return hashlib.sha256((s or '').encode()).hexdigest()[:16].encode()

def fetch_active_rows():
    sql = f"SELECT `{COL_NAME}`, `{COL_ADDR}` FROM `{TABLE}` WHERE `{COL_STATUS}` LIKE '%영업%'"
    with pymysql.connect(**MARIADB) as conn:
        df = pd.read_sql_query(sql, conn)
    return df.to_dict('records')

def main():
    rows = fetch_active_rows()
    if not rows:
        print("전송할 작업 없음"); return
    p = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks='all', linger_ms=50, retries=3,
        # 필요시 압축: compression_type='gzip',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    )
    sent = 0
    for r in rows:
        name, addr = r.get(COL_NAME), r.get(COL_ADDR)
        if not name: continue
        msg = {"type":"task","original_name":name,"address":addr,"source":"mariadb"}
        p.send(TOPIC, key=kkey(name), value=msg); sent += 1
    p.flush()
    print(f"✅ 작업 {sent}건 전송 → {TOPIC}")

if __name__ == "__main__":
    main()

