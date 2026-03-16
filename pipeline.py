import requests, json, time, os

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
MAKE_WEBHOOK = os.environ["MAKE_WEBHOOK_URL"]

SEARCH_INPUTS = {
  "cheap_scraper~linkedin-job-scraper": {
    "urls": [
      "https://www.linkedin.com/jobs/search/?keywords=junior+OR+trainee+OR+associate&location=Berlin&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=junior+OR+trainee+OR+associate&location=Munich&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=junior+OR+trainee+OR+associate&location=Hamburg&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=junior+OR+trainee+OR+associate&location=Frankfurt&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=junior+OR+trainee+OR+associate&location=Cologne&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=business+analyst+OR+operations+OR+supply+chain&location=Germany&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=marketing+OR+digital+OR+consulting&location=Germany&f_TPR=r7200&f_E=1,2,3,4",
      "https://www.linkedin.com/jobs/search/?keywords=junior+business&location=DACH&f_TPR=r7200&f_E=1,2,3,4"
    ],
    "count": 50
  }
}

GERMAN_REQUIRED_KEYWORDS = [
    "deutschkenntnisse erforderlich",
    "deutsch zwingend",
    "german is mandatory",
    "german required",
    "must speak german",
    "german language required",
    "verhandlungssicheres deutsch",
    "fließende deutschkenntnisse",
    "sehr gute deutschkenntnisse",
    "german: c1",
    "german: c2",
    "muttersprache deutsch"
]

GERMAN_OK_KEYWORDS = [
    "german is a plus",
    "german preferred",
    "basic german",
    "german b1",
    "german b2",
    "german is not required",
    "no german required",
    "english is sufficient",
    "english only",
    "working knowledge of german"
]

def filter_job(description):
    desc_lower = description.lower()
    
    # Fail if description is mostly German (common German words)
    german_word_count = sum([
        desc_lower.count(" und "),
        desc_lower.count(" die "),
        desc_lower.count(" der "),
        desc_lower.count(" das "),
        desc_lower.count(" wir "),
        desc_lower.count(" sie "),
        desc_lower.count(" mit ")
    ])
    if german_word_count > 10:
        return {"pass": False, "german_requirement": "German Description"}

    # Fail if German explicitly required
    for kw in GERMAN_REQUIRED_KEYWORDS:
        if kw in desc_lower:
            return {"pass": False, "german_requirement": "Required"}

    # Pass if explicitly OK without German
    for kw in GERMAN_OK_KEYWORDS:
        if kw in desc_lower:
            return {"pass": True, "german_requirement": "Not Required"}

    # Pass if description is in English
    return {"pass": True, "german_requirement": "Not Specified"}

def send_to_sheet(job, filter_result):
    payload = {
        "job_id": job.get("id", ""),
        "title": job.get("title", ""),
        "company": job.get("companyName", ""),
        "platform": "LinkedIn",
        "city": job.get("location", ""),
        "posted": job.get("postedAt", ""),
        "applicants": job.get("applicantsCount", ""),
        "german_req": filter_result.get("german_requirement", ""),
        "level": job.get("seniorityLevel", ""),
        "apply_link": job.get("link", ""),
        "status": "New",
        "custom_paragraph": "Paste job here in Claude.ai chat to get customized cover letter"
    }
    requests.post(MAKE_WEBHOOK, json=payload)

def main():
    seen_ids = set()
    all_jobs = []

    for actor_id, input_data in SEARCH_INPUTS.items():
        jobs = run_apify_actor(actor_id, input_data)
        for job in jobs:
            jid = job.get("id", "")
            if jid in seen_ids:
                continue
            seen_ids.add(jid)
            applicants = str(job.get("applicantsCount", "99"))
            try:
                if int(applicants) > 10:
                    continue
            except:
                pass
            all_jobs.append(job)

    print(f"Jobs after filter: {len(all_jobs)}")

    for job in all_jobs:
        desc = job.get("descriptionText", "")
        result = filter_job(desc)
        if not result.get("pass"):
            continue
        send_to_sheet(job, result)
        time.sleep(1)

def run_apify_actor(actor_id, input_data):
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_TOKEN}"
    r = requests.post(url, json=input_data)
    run_id = r.json()["data"]["id"]
    for _ in range(30):
        time.sleep(10)
        status = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        ).json()["data"]["status"]
        if status == "SUCCEEDED":
            break
    dataset_id = requests.get(
        f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    ).json()["data"]["defaultDatasetId"]
    items = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&limit=200"
    ).json()
    return items

if __name__ == "__main__":
    main()
