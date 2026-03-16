import requests, json, time, os

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
MAKE_WEBHOOK = os.environ["MAKE_WEBHOOK_URL"]

KEYWORDS = [
    "junior", "trainee", "associate", "graduate", "entry level",
    "business analyst", "project coordinator", "operations",
    "supply chain", "marketing", "digital", "data analyst",
    "product manager", "business development", "strategy",
    "consulting", "e-commerce", "procurement", "logistics",
    "finance analyst", "hr coordinator", "sales associate",
    "customer success", "program coordinator", "project management"
]

CITIES = [
    "Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne",
    "Stuttgart", "Dusseldorf", "Leipzig", "Dortmund", "Essen",
    "Bremen", "Dresden", "Hannover", "Nuremberg", "Mannheim",
    "Augsburg", "Wiesbaden", "Bonn", "Karlsruhe", "Munster",
    "Freiburg", "Heidelberg", "Kiel", "Mainz", "Erfurt"
]

SEARCH_URLS = []

KEYWORD_GROUPS = [
    "junior OR trainee OR associate OR graduate",
    "business+analyst OR operations OR supply+chain OR procurement OR logistics",
    "marketing OR digital OR consulting OR e-commerce OR strategy",
    "data+analyst OR project+manager OR business+development OR customer+success"
]

for group in KEYWORD_GROUPS:
    for city in CITIES[:5]:
        SEARCH_URLS.append(
            f"https://www.linkedin.com/jobs/search/?keywords={group}&location={city}&f_TPR=r7200&f_E=1,2,3,4"
        )
    for location in ["Germany", "DACH", "EMEA"]:
        SEARCH_URLS.append(
            f"https://www.linkedin.com/jobs/search/?keywords={group}&location={location}&f_TPR=r7200&f_E=1,2,3,4"
        )

SEARCH_INPUTS = {
    "cheap_scraper~linkedin-job-scraper": {
        "urls": SEARCH_URLS,
        "count": 50
    }
}

GERMAN_REQUIRED = [
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
    "muttersprache deutsch",
    "fließendes deutsch",
    "deutsch voraussetzung",
    "deutschkenntnisse vorausgesetzt",
    "german language is a must",
    "fluent german",
    "fluency in german is required",
    "german fluency required",
    "c1 german",
    "c2 german"
]

GERMAN_OK = [
    "german is a plus",
    "german is an advantage",
    "german preferred",
    "basic german",
    "german b1",
    "german b2",
    "german is not required",
    "no german required",
    "english is sufficient",
    "english only",
    "working knowledge of german",
    "german is beneficial",
    "german is desirable",
    "german is welcome",
    "knowledge of german is a plus"
]

GERMAN_TITLE_SYLLABLES = [
    "leiter", "leitung", "kaufmann", "kauffrau", "sachbearbeiter",
    "werkstudent", "referent", "mitarbeiter", "vertrieb", "einkauf",
    "buchhaltung", "praktikant", "ausbildung", "beratung", "entwicklung",
    "geschafts", "projekt", "abteilung", "verantwortlich", "stellvertreter"
]

def is_german_title(title):
    title_lower = title.lower()
    for syllable in GERMAN_TITLE_SYLLABLES:
        if syllable in title_lower:
            return True
    return False

def is_german_description(description):
    desc_lower = description.lower()
    german_words = [
        " und ", " die ", " der ", " das ", " wir ",
        " sie ", " mit ", " für ", " von ", " auf ",
        " ist ", " ein ", " eine ", " nicht ", " auch ",
        " bei ", " als ", " nach ", " aber ", " oder "
    ]
    count = sum(desc_lower.count(w) for w in german_words)
    return count > 15

def filter_job(title, description):
    desc_lower = description.lower()

    # Fail if title has German syllables
    if is_german_title(title):
        return {"pass": False, "reason": "German title"}

    # Fail if description is written in German
    if is_german_description(description):
        return {"pass": False, "reason": "German description"}

    # Fail if German explicitly required
    for kw in GERMAN_REQUIRED:
        if kw in desc_lower:
            return {"pass": False, "reason": "German required"}

    # Pass if German listed as plus/optional
    for kw in GERMAN_OK:
        if kw in desc_lower:
            return {"pass": True, "german_requirement": "German is a plus"}

    # Pass — English description, no German requirement found
    return {"pass": True, "german_requirement": "Not specified"}

def send_to_sheet(job, filter_result):
    payload = {
        "job_id": job.get("id", ""),
        "title": job.get("title", ""),
        "company": job.get("companyName", ""),
        "platform": "LinkedIn",
        "city": job.get("location", ""),
        "posted": job.get("postedAt", ""),
        "applicants": job.get("applicantsCount", ""),
        "german_req": filter_result.get("german_requirement", "Not specified"),
        "level": job.get("seniorityLevel", ""),
        "apply_link": job.get("link", ""),
        "status": "New",
        "custom_paragraph": "Paste this job in Claude.ai to get customized CV and cover letter"
    }
    requests.post(MAKE_WEBHOOK, json=payload)
    print(f"Sent: {job.get('title')} at {job.get('companyName')}")

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

def main():
    seen_ids = set()
    all_jobs = []

    for actor_id, input_data in SEARCH_INPUTS.items():
        print(f"Running scraper with {len(input_data['urls'])} URLs")
        jobs = run_apify_actor(actor_id, input_data)
        print(f"Raw jobs returned: {len(jobs)}")
        for job in jobs:
            jid = job.get("id", "")
            if jid in seen_ids:
                continue
            seen_ids.add(jid)
            all_jobs.append(job)

    print(f"Total unique jobs: {len(all_jobs)}")

    passed = 0
    for job in all_jobs:
        title = job.get("title", "")
        desc = job.get("descriptionText", "")
        result = filter_job(title, desc)
        if not result.get("pass"):
            continue
        passed += 1
        send_to_sheet(job, result)
        time.sleep(1)

    print(f"Jobs sent to sheet: {passed}")

if __name__ == "__main__":
    main()
