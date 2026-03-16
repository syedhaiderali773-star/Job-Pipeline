import requests, json, time, os
import anthropic

APIFY_TOKEN = os.environ["APIFY_TOKEN"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
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

GERMAN_FILTER_PROMPT = """
You are filtering job postings for an English-speaking candidate with German B1.
Read the job description and return ONLY a JSON object like this:
{"pass": true, "german_requirement": "Not Required", "reason": "one sentence"}
PASS if: description is in English OR German is optional/preferred/up to B1.
FAIL if: description is fully in German OR German is mandatory/required.
"""

CV_PROMPT = """
You are an ATS CV optimizer. Given a job description and candidate background,
write a customized cover letter opening paragraph of 3-4 sentences maximum.
Keep it concise, direct, and results-focused. Match the job keywords exactly.
Candidate: Syed Haider Ali, Business Graduate, 3 roles at Infineon Technologies Munich,
RPA automation reducing costs 70%, 75 FTE automation, SAP, JIRA, Confluence, Power BI,
data analytics, digital transformation, supply chain, operations,
M.Sc. Digital Technology Management Hochschule Munchen, English C2, German B1.
"""

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

def filter_job(description):
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=200,
        messages=[{"role": "user", "content": GERMAN_FILTER_PROMPT + "\n\nJob description:\n" + description[:3000]}]
    )
    try:
        return json.loads(msg.content[0].text)
    except:
        return {"pass": False, "german_requirement": "Unknown", "reason": "Parse error"}

def customize_cover(job_title, company, description):
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": CV_PROMPT + f"\n\nJob: {job_title} at {company}\nDescription:\n{description[:2000]}"}]
    )
    return msg.content[0].text.strip()

def send_to_sheet(job, filter_result, custom_paragraph):
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
        "custom_paragraph": custom_paragraph
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
        custom = customize_cover(job.get("title", ""), job.get("companyName", ""), desc)
        send_to_sheet(job, result, custom)
        time.sleep(1)

if __name__ == "__main__":
    main()
