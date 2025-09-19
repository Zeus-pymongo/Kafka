# test_consumer.py  — topic_test → Selenium → MongoDB
import os, json, time
from kafka import KafkaConsumer
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ==== 환경 ====
KAFKA_BOOTSTRAP = '192.168.0.223:9092'
KAFKA_TOPIC = 'topic_test' # 카프카에서 토픽 생성한 걸로 변경
GROUP_ID = 'topic_group_test' ## consumer 실행 시 자동으로 group_id 생성
CLIENT_ID = os.getenv("CLIENT_ID") or None

MONGO_URI = "mongodb://kevin:pass123%23@192.168.0.222:27017/"
DB_NAME = "jongro"
COLL_NAME = "ll"   # 원하는 컬렉션명으로

# ==== 프로듀서 메시지 키 매핑 (test_producer.py가 보내는 키) ====
MARIADB_COLUMN = 'original_name'
MARIADB_ADDRESS_COLUMN = 'address'

# ==== Chrome Driver ====
def build_driver():
    service = Service(ChromeDriverManager().install())
    opts = webdriver.ChromeOptions()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=service, options=opts)

def build_mongo():
    mongo = MongoClient(MONGO_URI)
    return mongo, mongo[DB_NAME][COLL_NAME]

# ==== parse_apollo_data: minseonjae 쪽과 동일한 스펙으로 상세 수집 ====
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
        prices = []
        for m in menus:
            p = ''.join(ch for ch in str(m.get('price','0')) if ch.isdigit())
            if p.isdigit(): prices.append(int(p))
        mains = [p for p in prices if 5000 <= p <= 80000]
        data['avg_price'] = round(sum(mains)/len(mains),2) if mains else 0.0

        # 키워드
        voted = []
        stats_key = next((k for k in apollo_data if k.startswith('VisitorReviewStatsResult:')), None)
        if stats_key:
            details = (((apollo_data.get(stats_key,{}) or {}).get('analysis',{}) or {}).get('votedKeyword',{}) or {}).get('details',[]) or []
            for d in details:
                dn, ct = d.get('displayName'), d.get('count')
                if dn and (ct is not None):
                    voted.append({'keyword':dn,'count':ct})
        data['voted_keywords'] = voted
        return data
    except Exception:
        return None

# ==== 크롤 핵심 (iframe 진입/후보 클릭 → __APOLLO_STATE__ → 파싱 → 업서트) ====
def crawl_with_driver(driver, coll, info):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

    name = info.get(MARIADB_COLUMN)
    addr = info.get(MARIADB_ADDRESS_COLUMN)
    cleaned_name = (name or "").strip(' "')

    search_url = f"https://map.naver.com/p/search/종로구 {cleaned_name}"
    driver.get(search_url)

    LIST = "#_pcmap_list_scroll_container > ul > li"
    ADDR = "span.CTXwV"
    LINK = "a.place_bluelink"

    # 1) 상세 iframe 바로 보이면 전환
    try:
        WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
    except TimeoutException:
        # 2) 검색 목록 프레임에서 후보 클릭
        WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))
        try:
            try:
                WebDriverWait(driver, 2).until(EC.invisibility_of_element_located((By.ID, "entryIframe")))
            except TimeoutException:
                pass

            dong_info = None
            if addr:
                for part in addr.split():
                    if part.endswith('동') or part.endswith('가'):
                        dong_info = part
                        break

            target_to_click = None
            results = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, LIST))
            )[:5]

            for r in results:
                try:
                    naver_address = r.find_element(By.CSS_SELECTOR, ADDR).text
                    if (not dong_info) or (dong_info in naver_address):
                        target_to_click = r.find_element(By.CSS_SELECTOR, LINK)
                        break
                except NoSuchElementException:
                    continue

            if target_to_click is None:
                target_to_click = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, f"{LIST}:first-child {LINK}"))
                )

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target_to_click)
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(target_to_click))
            try:
                driver.execute_script("arguments[0].click();", target_to_click)
            except ElementClickInterceptedException:
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", target_to_click)

            time.sleep(1.5)
            driver.switch_to.default_content()
            WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
        except TimeoutException:
            return {'original_name': name, 'name': cleaned_name, 'status': 'fail', 'reason': '페이지 탐색/클릭 실패'}

    # 3) 상세 JSON 추출/파싱
    apollo_data = driver.execute_script("return window.__APOLLO_STATE__;")
    parsed = parse_apollo_data(apollo_data)
    if not parsed:
        return {'original_name': name, 'name': cleaned_name, 'status': 'fail', 'reason': 'JSON 파싱 실패'}

    parsed['original_name'] = name
    parsed['address'] = addr
    parsed['status'] = 'success'

    # 4) 업서트
    coll.update_one(
        {'original_name': name, 'address': addr},
        {'$set': parsed},
        upsert=True
    )
    return parsed

def main():
    mongo, coll = build_mongo()
    driver = build_driver()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        client_id=CLIENT_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        max_poll_records=1,
        max_poll_interval_ms=1800000,
        session_timeout_ms=30000,
        request_timeout_ms=305000,
    )

    try:
        for msg in consumer:
            payload = msg.value or {}
            name = payload.get("original_name")
            addr = payload.get("address")
            if not name:
                consumer.commit()
                continue

            info = {MARIADB_COLUMN: name, MARIADB_ADDRESS_COLUMN: addr}

            try:
                result = crawl_with_driver(driver, coll, info)
                print(f"[consume] p={msg.partition} off={msg.offset} name={name} status={(result or {}).get('status')}")
                consumer.commit()
            except Exception as e:
                print(f"[ERROR] p={msg.partition} off={msg.offset} name={name} err={e}")
                try:
                    driver.quit()
                except:
                    pass
                driver = build_driver()
    finally:
        try: driver.quit()
        except: pass
        consumer.close(); mongo.close()

if __name__ == "__main__":
    main()

