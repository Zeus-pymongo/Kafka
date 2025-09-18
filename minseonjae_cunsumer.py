# minseonjae_cunsumer.py (재사용형)
import os, json, time
from kafka import KafkaConsumer
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# 네 크롤러 파일명으로 바꾸세요
from minseonjae_producer import (
    MARIADB_COLUMN, MARIADB_ADDRESS_COLUMN,
    parse_apollo_data
)

# ---- 환경 ----
KAFKA_BOOTSTRAP = '192.168.0.223:9092'
KAFKA_TOPIC = 'crawl.raw'
GROUP_ID = 'mongo-writer'
CLIENT_ID = os.getenv("CLIENT_ID") or None

MONGO_URI = "mongodb://kevin:pass123%23@192.168.0.222:27017/"
DB_NAME = "jongro"
COLL_NAME = "RESTAURANTS_GENERAL"

def build_driver():
    service = Service(ChromeDriverManager().install())
    opts = webdriver.ChromeOptions()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(service=service, options=opts)

def build_mongo():
    mongo = MongoClient(MONGO_URI)
    return mongo, mongo[DB_NAME][COLL_NAME]

def crawl_with_driver(driver, coll, info):
    """
    worker_crawl()의 핵심만 재구성:
    - driver/collection을 인자로 받아 '재사용'
    - 페이지 진입 → window.__APOLLO_STATE__ → parse_apollo_data() → 업서트
    """
    name = info.get(MARIADB_COLUMN)
    addr = info.get(MARIADB_ADDRESS_COLUMN)
    cleaned_name = (name or "").strip(' "')

    # 1) 검색 페이지
    search_url = f"https://map.naver.com/p/search/종로구 {cleaned_name}"
    driver.get(search_url)

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

    SEARCH_RESULT_LIST_SELECTOR = "#_pcmap_list_scroll_container > ul > li"
    SEARCH_RESULT_ADDRESS_SELECTOR = "span.CTXwV"
    SEARCH_RESULT_LINK_SELECTOR = "a.place_bluelink"

    # 2) 상세 iframe 바로 보이면 전환
    try:
        WebDriverWait(driver, 5).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "entryIframe")))
    except TimeoutException:
        # 검색 목록 프레임에서 후보 클릭
        WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.ID, "searchIframe")))
        try:
            # entryIframe가 겹치면 조금 기다림
            try:
                WebDriverWait(driver, 2).until(EC.invisibility_of_element_located((By.ID, "entryIframe")))
            except TimeoutException:
                pass

            # 동/가 단서 추출
            dong_info = None
            if addr:
                for part in addr.split():
                    if part.endswith('동') or part.endswith('가'):
                        dong_info = part
                        break

            # 상위 5개 중 주소 매칭이 있으면 그걸 클릭
            target_to_click = None
            results = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, SEARCH_RESULT_LIST_SELECTOR))
            )[:5]

            for r in results:
                try:
                    naver_address = r.find_element(By.CSS_SELECTOR, SEARCH_RESULT_ADDRESS_SELECTOR).text
                    if (not dong_info) or (dong_info in naver_address):
                        target_to_click = r.find_element(By.CSS_SELECTOR, SEARCH_RESULT_LINK_SELECTOR)
                        break
                except NoSuchElementException:
                    continue

            if target_to_click is None:
                target_to_click = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, f"{SEARCH_RESULT_LIST_SELECTOR}:first-child {SEARCH_RESULT_LINK_SELECTOR}"))
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
    # 재사용 자원 생성
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
                # 드라이버가 죽었을 수도 있으니 재생성 시도
                try:
                    driver.quit()
                except:
                    pass
                driver = build_driver()
    finally:
        try:
            driver.quit()
        except:
            pass
        consumer.close()
        mongo.close()

if __name__ == "__main__":
    main()

