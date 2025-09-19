# crawler_producer.py
import time, json, os, sys, hashlib
import pymysql, pandas as pd
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from multiprocessing import Pool
from tqdm import tqdm

# Kafka (옵션)
from kafka import KafkaProducer

# ===== 설정 =====
MARIADB_CONFIG = {'host':'192.168.0.221','port':3306,'user':'jongro','password':'pass123#','db':'jongro','charset':'utf8'}
MARIADB_TABLE = 'RESTAURANTS_GENERAL'
MARIADB_COLUMN = 'STORE_NAME'
MARIADB_ADDRESS_COLUMN = 'DETAIL_ADD'
MARIADB_STATUS_COLUMN = 'OP_STATUS'

MONGO_CONFIG = {'host':'192.168.0.222','port':27017,'username':'kevin','password':'pass123#','db_name':'jongro'}
MONGO_COLLECTION = 'RESTAURANTS_GENERAL'

NUM_PROCESSES = 2

# Kafka
KAFKA_BOOTSTRAP = '192.168.0.223:9092'
KAFKA_TOPIC = 'crawl.raw'
KAFKA_FORWARD = os.getenv("KAFKA_FORWARD") == "1"     # ← 크롤링 결과도 Kafka로 보낼지 여부
PRODUCE_TASKS = os.getenv("PRODUCE_TASKS") == "1"     # ← MariaDB 작업을 Kafka에만 넣고 종료

# ===== DB 유틸 =====
def get_restaurant_list_from_mariadb():
    try:
        conn = pymysql.connect(**MARIADB_CONFIG)
        q = f"SELECT `{MARIADB_COLUMN}`, `{MARIADB_ADDRESS_COLUMN}` FROM `{MARIADB_TABLE}` WHERE `{MARIADB_STATUS_COLUMN}` LIKE '%영업%'"
        df = pd.read_sql_query(q, conn)
        conn.close()
        print(f"✅ MariaDB에서 '영업' 중인 {len(df)}건")
        return df.to_dict('records')
    except Exception as e:
        print(f"❌ MariaDB 실패: {e}"); return []

def get_mongodb_collection():
    client = None
    try:
        if MONGO_CONFIG['username'] and MONGO_CONFIG['password']:
            uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        else:
            client = MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'], serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[MONGO_CONFIG['db_name']]
        return db[MONGO_COLLECTION], client
    except Exception as e:
        print(f"❌ MongoDB 연결 실패: {e}")
        if client: client.close()
        return None, None

def get_already_crawled_list():
    coll, client = get_mongodb_collection()
    if coll is None: return set()
    try:
        names = set(doc['original_name'] for doc in coll.find({}, {'original_name':1}) if doc.get('original_name'))
        print(f"✅ 기수집 {len(names)}건")
        return names
    except Exception as e:
        print(f"❌ Mongo 조회 실패: {e}")
        return set()
    finally:
        if client: client.close()

# ===== 파싱 =====
def parse_apollo_data(apollo_data):
    try:
        data = {}
        base_key = next((k for k in apollo_data if k.startswith('PlaceDetailBase:')), None)
        if not base_key: raise ValueError('PlaceDetailBase 없음')
        base = apollo_data[base_key]
        data['name'] = base.get('name','이름 정보 없음')
        data['category'] = base.get('category','업태 정보 없음')
        micro = base.get('microReviews',[])
        data['review_summary'] = ', '.join(micro) if micro else '리뷰 요약 없음'
        vtot = base.get('visitorReviewsTotal','0')
        data['visitor_reviews'] = int(str(vtot).replace(',','')) if vtot else 0
        data['rating'] = base.get('visitorReviewsScore',0.0)
        # 블로그 리뷰 수
        root = apollo_data.get('ROOT_QUERY',{})
        fsas_key = next((k for k in root if k.startswith('fsasReviews({')), None)
        total = '0'
        if fsas_key:
            total = root[fsas_key].get('total','0')
        data['blog_reviews'] = int(str(total).replace(',','')) if total else 0
        # 메뉴
        menus = []
        for k,v in apollo_data.items():
            if isinstance(v,dict) and v.get('__typename')=='Menu' and v.get('name') and v.get('price'):
                menus.append({'item':v['name'],'price':v['price']})
        data['menus'] = menus
        prices = [int(str(m.get('price','0')).replace(',','')) for m in menus if str(m.get('price','0')).isdigit()]
        mains = [p for p in prices if 5000 <= p <= 80000]
        data['avg_price'] = round(sum(mains)/len(mains),2) if mains else 0.0
        # 키워드
        voted = []
        stats_key = next((k for k in apollo_data if k.startswith('VisitorReviewStatsResult:')), None)
        if stats_key:
            dets = apollo_data.get(stats_key,{}).get('analysis',{}).get('votedKeyword',{}).get('details',[])
            for d in dets:
                if 'displayName' in d and 'count' in d:
                    voted.append({'keyword':d['displayName'],'count':d['count']})
        data['voted_keywords'] = voted
        return data
    except Exception:
        return None

# ===== Kafka 유틸 =====
_kafka_producer = None

def _kafka_key(s: str) -> bytes:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()[:16].encode('utf-8')

def _get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            acks='all', retries=5, linger_ms=50,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        )
    return _kafka_producer

def send_tasks_to_kafka(tasks):
    p = _get_kafka_producer()
    sent = 0
    for t in tasks:
        name = t.get(MARIADB_COLUMN); addr = t.get(MARIADB_ADDRESS_COLUMN)
        if not name: continue
        msg = {"type":"task","original_name":name,"address":addr,"source":"mariadb","requested_at":pd.Timestamp.utcnow().isoformat()}
        p.send(KAFKA_TOPIC, key=_kafka_key(name), value=msg); sent += 1
    p.flush()
    print(f"✅ Kafka 전송 완료: {sent}건 (topic={KAFKA_TOPIC})")

def send_result_to_kafka(doc):
    try:
        p = _get_kafka_producer()
        payload = dict(doc)
        payload['type'] = 'parsed'
        p.send(KAFKA_TOPIC, key=_kafka_key(payload.get('original_name','')), value=payload)
    except Exception as e:
        print(f"[WARN] Kafka 전송 실패: {e}")

# ===== 크롤러 워커 =====
def worker_crawl(info):
    restaurant_name = info[MARIADB_COLUMN]
    restaurant_address = info.get(MARIADB_ADDRESS_COLUMN)
    driver = None; mongo_client = None
    cleaned_name = restaurant_name.strip(' "')
    LIST = "#_pcmap_list_scroll_container > ul > li"
    ADDR = "span.CTXwV"; LINK = "a.place_bluelink"
    try:
        service = Service(ChromeDriverManager().install())
        opts = webdriver.ChromeOptions()
        opts.add_argument('--headless')
        opts.add_argument('--no-sandbox'); opts.add_argument('--disable-dev-shm-usage'); opts.add_argument('--disable-gpu')
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
        driver = webdriver.Chrome(service=service, options=opts)

        coll, mongo_client = get_mongodb_collection()
        if coll is None: raise Exception("MongoDB 컬렉션 연결 실패")

        search_url = f"https://map.naver.com/p/search/종로구 {cleaned_name}"
        driver.get(search_url)
        try:
            WebDriverWait(driver,5).until(EC.frame_to_be_available_and_switch_to_it((By.ID,"entryIframe")))
        except TimeoutException:
            WebDriverWait(driver,10).until(EC.frame_to_be_available_and_switch_to_it((By.ID,"searchIframe")))
            try:
                try: WebDriverWait(driver,2).until(EC.invisibility_of_element_located((By.ID,"entryIframe")))
                except TimeoutException: pass
                dong = None
                if restaurant_address:
                    for part in restaurant_address.split():
                        if part.endswith('동') or part.endswith('가'):
                            dong = part; break
                target = None
                results = WebDriverWait(driver,10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, LIST)))[:5]
                for r in results:
                    try:
                        nv_addr = r.find_element(By.CSS_SELECTOR, ADDR).text
                        if (not dong) or (dong in nv_addr):
                            target = r.find_element(By.CSS_SELECTOR, LINK); break
                    except NoSuchElementException:
                        continue
                if target is None:
                    target = WebDriverWait(driver,10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"{LIST}:first-child {LINK}")))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
                WebDriverWait(driver,5).until(EC.element_to_be_clickable(target))
                try: driver.execute_script("arguments[0].click();", target)
                except ElementClickInterceptedException:
                    time.sleep(0.5); driver.execute_script("arguments[0].click();", target)
                time.sleep(2); driver.switch_to.default_content()
                WebDriverWait(driver,10).until(EC.frame_to_be_available_and_switch_to_it((By.ID,"entryIframe")))
            except TimeoutException:
                return {'original_name':restaurant_name,'name':cleaned_name,'status':'fail','reason':'페이지 탐색/클릭 실패'}
        apollo = driver.execute_script("return window.__APOLLO_STATE__;")
        parsed = parse_apollo_data(apollo)
        if parsed:
            parsed['original_name'] = restaurant_name
            parsed['address'] = restaurant_address
            parsed['status'] = 'success'
            coll.update_one({'original_name':restaurant_name,'address':restaurant_address},{'$set':parsed},upsert=True)
            # ✅ 크롤링 성공 시 Kafka에도 전송(옵션)
            if KAFKA_FORWARD:
                send_result_to_kafka(parsed)
            return parsed
        else:
            return {'original_name':restaurant_name,'name':cleaned_name,'status':'fail','reason':'JSON 파싱 실패'}
    except Exception as e:
        return {'original_name':restaurant_name,'name':cleaned_name,'status':'error','reason':str(e)}
    finally:
        if driver: driver.quit()
        if mongo_client: mongo_client.close()

# ===== 진입점 =====
if __name__ == "__main__" and PRODUCE_TASKS:
    tasks = get_restaurant_list_from_mariadb()
    if not tasks: print("❌ 전송할 작업 없음"); sys.exit(0)
    send_tasks_to_kafka(tasks); sys.exit(0)

if __name__ == "__main__":
    total = get_restaurant_list_from_mariadb()
    if not total:
        print("작업 목록 없음. 종료"); sys.exit(0)
    done = get_already_crawled_list()
    tasks = [info for info in total if info[MARIADB_COLUMN] not in done]
    if not tasks:
        print("모든 데이터 수집 완료. 종료"); sys.exit(0)
    print(f"총 {len(total)}개 중 {len(done)}개 제외, {len(tasks)}개 크롤링 시작")
    results = []
    with Pool(processes=NUM_PROCESSES) as pool:
        with tqdm(total=len(tasks), desc="Crawling Progress") as pbar:
            for res in pool.imap_unordered(worker_crawl, tasks):
                results.append(res)
                if res and res.get('status') == 'success':
                    pbar.set_description(f"Crawling Progress (Last: {res.get('name')})")
                else:
                    if res:
                        pbar.write(f"[실패] {res.get('original_name','?')}: {res.get('reason','?')}")
                    else:
                        pbar.write(f"[실패] worker None 반환")
                pbar.update(1)
    ok = sum(1 for r in results if r and r.get('status')=='success')
    print(f"\n--- 완료 --- 성공 {ok} / 실패 {len(results)-ok}")

