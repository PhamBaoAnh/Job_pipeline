import requests
import json

url = "https://ms.vietnamworks.com/job-search/v1.0/search"
all_jobs = []

hits_per_page = 20
page = 0

while True:
    payload = {
        "query": "data engineer",
        "filter": [],
        "ranges": [],
        "order": [],
        "hitsPerPage": hits_per_page,
        "page": page
    }

    response = requests.post(url, json=payload)
    data = response.json()

    job_list = data.get("data", [])
    meta = data.get("meta", {})
    total_pages = meta.get("nbPages", 0)

    print(f"📄 Lấy trang {page + 1}/{total_pages} với {len(job_list)} job")

    all_jobs.extend(job_list)

    page += 1
    if page >= total_pages:
        break

# ✅ Lưu ra file JSON (hoặc có thể convert thành Excel/CSV)
with open("vietnamworks_jobs_full.json", "w", encoding="utf-8") as f:
    json.dump(all_jobs, f, indent=2, ensure_ascii=False)

print(f"\n✅ Hoàn tất. Tổng số job lấy được: {len(all_jobs)}")
