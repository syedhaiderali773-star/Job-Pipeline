import requests, json, time, os

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
MAKE_WEBHOOK = os.environ["MAKE_WEBHOOK_URL"]

GERMAN_REQUIRED = [
    "deutschkenntnisse erforderlich",
    "deutsch zwingend",
    "german is mandatory",
    "german required",
    "must speak german",
    "german language required",
    "verhandlungssicheres deutsch",
    "fliessende deutschkenntnisse",
    "sehr gute deutschkenntnisse",
    "german: c1", "german: c2",
    "muttersprache deutsch",
    "fliessendes deutsch",
    "deutsch voraussetzung",
    "deutschkenntnisse vorausgesetzt",
    "german language is a must",
    "fluent german is required",
    "fluency in german is required",
    "german fluency required",
    "c1 german", "c2 german",
    "german speaker required",
    "native german"
]

GERMAN_OK = [
    "german is a plus",
    "german is an advantage",
    "german preferred",
    "basic german",
    "german b1", "german b2",
    "german is not required",
    "no german required",
    "english is sufficient",
    "english only",
    "working knowledge of german",
    "german is beneficial",
    "german is desirable",
    "german is welcome",
    "knowledge of german is a plus",
    "german is optional",
    "german would be a plus"
]

def is_german_text(text):
    german_words = [
        " und ", " die ", " der ", " das ", " wir ",
        " sie ", " mit ", " fur ", " von ", " auf ",
        " ist ", " ein ", " eine ", " nicht ", " auch ",
        " bei ", " als ", " nach ", " aber ", " oder ",
        " werden ", " haben ", " durch ", " ihrer ",
        " konnen ", " ihrem ", " diesem ", " dieser "
    ]
    count = sum(text.lower().count(w) for w in german_words)
    total_words = len(text.split())
    if total_words == 0:
        return False
    return (count / total_words) > 0.05

def filter_job(title, description):
    desc_lower = description.lower()

    if is_german_text(title):
        return {"pass": False, "reason": "German title"}

    if is_german_text(description):
        return {"pass": False, "reason": "German description"}

    for kw in GERMAN_REQUIRED:
        if kw in desc_lower:
            return {"pass": False, "reason": "German required"}

    for kw in GERMAN_OK:
        if kw in desc_lower:
            return {"pass": True, "german_requirement": "German is a plus"}

    return {"pass": True, "german_requirement": "Not specified"}

def send_to_sheet(job, filter_result, platform):
    payload = {
        "job_id": job.get("id", ""),
        "title": job.get("title", ""),
        "company": job.get("companyName", job.get("company", "")),
        "platform": platform,
        "city": job.get("location", ""),
        "posted": job.get("postedAt", job.get("date", "")),
        "applicants": job.get("applicantsCount", ""),
        "german_req": filter_result.get("german_requirement", "Not specified"),
        "level": job.get("seniorityLevel", ""),
        "apply_link": job.get("link", job.get("url", job.get("applyUrl", ""))),
        "status": "New",
        "custom_paragraph": "Paste this job into Claude.ai to get customized CV and cover letter"
    }
    requests.post(MAKE_WEBHOOK, json=payload)
    print(f"Sent: {payload['title']} at {payload['company']} [{platform}]")

def run_actor(actor_id, input_data):
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_TOKEN}"
    r = requests.post(url, json=input_data)
    if r.status_code != 201:
        print(f"Failed to start actor {actor_id}: {r.text}")
        return []
    run_id = r.json()["data"]["id"]
    for _ in range(40):
        time.sleep(15)
        status_r = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        ).json()
        status = status_r["data"]["status"]
        print(f"Actor {actor_id} status: {status}")
        if status in ["SUCCEEDED", "FAILED", "ABORTED"]:
            break
    if status != "SUCCEEDED":
        print(f"Actor {actor_id} did not succeed: {status}")
        return []
    dataset_id = status_r["data"]["defaultDatasetId"]
    items = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&limit=500"
    ).json()
    return items if isinstance(items, list) else []

def scrape_linkedin():
    print("Starting LinkedIn scraper...")
    keyword_groups = [
        "junior OR trainee OR associate OR graduate",
        "business analyst OR operations OR supply chain OR procurement OR logistics",
        "marketing OR digital OR consulting OR e-commerce OR strategy",
        "data analyst OR project manager OR business development OR customer success"
    ]
    urls = []
    for group in keyword_groups:
        for location in ["Germany", "DACH", "EMEA"]:
            urls.append(
                f"https://www.linkedin.com/jobs/search/?keywords={group}&location={location}&f_TPR=r43200&f_E=1,2,3,4"
            )
    jobs = run_actor("curious_coder~linkedin-jobs-scraper", {
        "urls": urls,
        "count": 25,
        "scrapeCompany": False
    })
    print(f"LinkedIn raw jobs: {len(jobs)}")
    return jobs, "LinkedIn"

def scrape_stepstone():
    print("Starting StepStone scraper...")
    keywords = [
        "junior business analyst", "trainee operations",
        "associate marketing", "junior supply chain",
        "junior project manager", "associate consulting",
        "junior data analyst", "graduate business"
    ]
    all_jobs = []
    for kw in keywords:
        jobs = run_actor("memo23~stepstone-search-cheerio-ppr", {
            "keyword": kw,
            "location": "Deutschland",
            "maxItems": 15
        })
        all_jobs.extend(jobs)
        time.sleep(2)
    print(f"StepStone raw jobs: {len(all_jobs)}")
    return all_jobs, "StepStone"

def scrape_arbeitsagentur():
    print("Starting Arbeitsagentur scraper...")
    keywords = [
        "junior business analyst", "trainee marketing",
        "associate operations", "junior supply chain",
        "junior consultant", "junior project manager"
    ]
    all_jobs = []
    for kw in keywords:
        jobs = run_actor("fatihtahta~arbeitsagentur-scraper", {
            "keyword": kw,
            "location": "Deutschland",
            "maxItems": 15
        })
        all_jobs.extend(jobs)
        time.sleep(2)
    print(f"Arbeitsagentur raw jobs: {len(all_jobs)}")
    return all_jobs, "Arbeitsagentur"

def main():
    seen_ids = set()
    total_sent = 0

    sources = [
        scrape_linkedin,
        scrape_stepstone,
        scrape_arbeitsagentur
    ]

    for scrape_fn in sources:
        try:
            jobs, platform = scrape_fn()
        except Exception as e:
            print(f"Error in {scrape_fn.__name__}: {e}")
            continue

        for job in jobs:
            jid = str(job.get("id", job.get("url", job.get("title", ""))))
            if jid in seen_ids or not jid:
                continue
            seen_ids.add(jid)

            title = job.get("title", "")
            desc = job.get("descriptionText", job.get("description", job.get("fullDescription", "")))

            if not title or not desc:
                continue

            result = filter_job(title, desc)
            if not result.get("pass"):
                print(f"Filtered: {title} — {result.get('reason')}")
                continue

            try:
                send_to_sheet(job, result, platform)
                total_sent += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"Error sending to sheet: {e}")

    print(f"\nDone. Total jobs sent to sheet: {total_sent}")

if __name__ == "__main__":
    main()
