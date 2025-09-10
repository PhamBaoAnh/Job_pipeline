import time
import random
import json
import re
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumbase import Driver

min_wait = 1
max_wait = 5

class JobDetailsInformation():
    def __init__(self):
        self.salary = "Negotiation"
        self.position = None
        self.by_expiration_date = None
        self.views = 0
        self.city = None
        self.deadline_submit = None
        self.description = None
        self.requirements = None
        self.work_time = None
        self.posted_date = None
        self.level = None
        self.field = None
        self.skills = None
        self.main_industry = list()
        self.cv_language = None
        self.yoe = None
        self.num_of_recruit = None
        self.work_form = None
        self.gender_require = None
        self.relation_fields = list()
        self.work_address = None
        self.company_size = None
        self.major_field = None
        self.key_words = list()
        self.area = list()

def safe_get_text(driver, xpath, timeout=5):
    try:
        elem = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return elem.text.strip()
    except:
        return None

def run_topcv_scraper():
    key_word = 'data engineer'
    driver = Driver(uc=True, headed=False)
    driver.set_window_size(1920, 1080)

    try:
        print("🔍 Đang mở trang tìm kiếm việc làm...")
        driver.uc_open_with_reconnect("https://www.topcv.vn/viec-lam-it", reconnect_time=6)
        time.sleep(3)

        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'keyword'))
        )

        for c in key_word:
            search_box.send_keys(c)
            time.sleep(random.uniform(0.2, 1))

        search_box.send_keys(Keys.RETURN)
        time.sleep(5)
        driver.uc_gui_click_captcha()
        time.sleep(3)

        # ✅ Check tổng số job
        total_jobs_text = safe_get_text(driver, '//h1[contains(@class,"job-found")]')
        print("📌 Tổng số job hiển thị:", total_jobs_text)

        total_jobs = None
        if total_jobs_text:
            match = re.search(r'\d+', total_jobs_text.replace('.', ''))
            if match:
                total_jobs = int(match.group())
                print(f"📌 Tổng số job (đã parse): {total_jobs}")

        # Phân trang
        try:
            pages = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '//ul[contains(@class,"pagination")]//a'))
            )
        except Exception:
            print("⚠️ Không tìm thấy phân trang. Có thể chỉ có 1 trang hoặc cấu trúc đã đổi.")
            pages = []

        next_pages = {i: p.get_attribute('href') for i, p in enumerate(pages) if p.get_attribute('href')} if pages else {0: driver.current_url}
        print("📑 Tổng số trang phân trang:", len(next_pages))

        details_link = {}
        idx = 0

        for page_idx in next_pages:
            print(f"📄 Đang xử lý trang {page_idx + 1}")
            driver.uc_open_with_reconnect(next_pages[page_idx], reconnect_time=6)
            time.sleep(2)

            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                ActionChains(driver).scroll_by_amount(0, 10000).perform()
                time.sleep(1)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            job_blocks = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.box-header'))
            )

            for job in job_blocks:
                try:
                    job_title = job.find_element(By.XPATH, './/h3//a')
                    job_title_text = job_title.find_element(By.XPATH, './/span').text
                    company_name = job.find_element(By.XPATH, './/a[@class="company"]').text
                    update_time = job.find_element(By.XPATH, './/label[@class="deadline"]').text
                    city = job.find_element(By.XPATH, './/div[@class="label-content"]//label[@class="address"]').text
                    remain_time = job.find_element(By.XPATH, './/div[@class="label-content"]//label[@class="time"]').text

                    details_link[idx] = {
                        'href': job_title.get_attribute('href'),
                        'title': job_title_text,
                        'company_name': company_name,
                        'update_time': update_time,
                        'remain_time': remain_time
                    }
                    idx += 1

                except Exception as e:
                    print(f"⚠️ Lỗi khi lấy job: {e}")

        print("📋 Tổng số job thu được:", len(details_link))

        results = []
        for idx, data in details_link.items():
            print(f"🔎 Đang lấy chi tiết job {idx}: {data['href']}")
            try:
                driver.uc_open_with_reconnect(data['href'], reconnect_time=6)
                time.sleep(5)

                details = JobDetailsInformation()
                details.salary = safe_get_text(driver, '//div[@class="job-detail__info--sections"]//div[1]//div[@class="job-detail__info--section-content-value"]')
                details.city = safe_get_text(driver, '//div[@class="job-detail__info--sections"]//div[2]//div[@class="job-detail__info--section-content-value"]')
                details.yoe = safe_get_text(driver, '//div[@class="job-detail__info--sections"]//div[3]//div[@class="job-detail__info--section-content-value"]')
                details.deadline_submit = safe_get_text(driver, '//div[@class="job-detail__info--deadline"]')
                details.description = safe_get_text(driver, '//h3[text()="Mô tả công việc"]/following-sibling::div')
                details.requirements = safe_get_text(driver, '//h3[text()="Yêu cầu ứng viên"]/following-sibling::div')
                details.work_address = safe_get_text(driver, '//h3[text()="Địa điểm làm việc"]/following-sibling::div')
                details.company_size = safe_get_text(driver, '//div[text()[contains(.,"Quy mô")]]/following-sibling::div[contains(@class,"company-value")]')
                details.major_field = safe_get_text(driver, '//div[contains(@class,"company-field")]//div[@class="company-value"]')


                try:
                    tags = driver.find_elements(By.XPATH, '//div[@class="job-tags"]//span')
                    details.main_industry = [t.text for t in tags]
                except: pass

                try:
                    general_info = driver.find_elements(By.XPATH, '//div[@class="box-general-content"]//div[@class="box-general-group-info-value"]')
                    details.level = general_info[0].text
                    details.num_of_recruit = general_info[2].text
                    details.work_form = general_info[3].text
                    details.gender_require = general_info[4].text
                except: pass

                try:
                    skills = driver.find_elements(By.XPATH, '//div[@class="box-category-tags"]//a')
                    details.skills = [s.text for s in skills]
                except: pass

                try:
                    area = driver.find_elements(By.XPATH, '//div[@class="box-category"]//div[@class="box-category-tags"]//span//a')
                    details.area = [a.text for a in area]
                    details.relation_fields = [s.text for s in skills[:len(skills)-len(area)]]
                except: pass

                results.append({
                    'id': str(idx),
                    'title': data['title'],
                    'company_name': data['company_name'],
                    'salary': details.salary,
                    'city': details.city,
                    'yoe': details.yoe,
                    'deadline_submit': details.deadline_submit,
                    'main_industry': details.main_industry,
                    'description': details.description,
                    'requirements': details.requirements,
                    'work_address': details.work_address,
                    'company_size': details.company_size,
                    'major_field': details.major_field,
                    'level': details.level,
                    'num_of_recruit': details.num_of_recruit,
                    'work_form': details.work_form,
                    'gender_require': details.gender_require,
                    'relation_fields': details.relation_fields,
                    'skills': details.skills,
                    'area': details.area,
                    'update_time': data['update_time'],
                    'remain_time': data['remain_time'],
                })

            except Exception as e:
                print(f"❌ Lỗi khi lấy chi tiết job {idx}: {e}")

        print("📥 Tổng số job đã lưu thành công:", len(results))

        if results:
            filename = f"topcv_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=4)
            print(f"✅ Đã lưu {len(results)} job vào file: {filename}")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_topcv_scraper()
