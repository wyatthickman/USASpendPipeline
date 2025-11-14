import os
import json
import requests
from datetime import date

# --- Config from env vars ---

PBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
PBI_DATASET_ID = os.environ["PBI_DATASET_ID"]
PBI_TABLE_NAME = os.environ.get("PBI_TABLE_NAME", "Awards")

def get_access_token():
    token_url = f"https://login.microsoftonline.com/{PBI_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": PBI_CLIENT_ID,
        "client_secret": PBI_CLIENT_SECRET,
        "scope": PBI_SCOPE,
        "grant_type": "client_credentials"
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def get_usaspending_page(page: int, start_date: str, end_date: str):
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    body = {
        "subawards": False,
        "page": page,
        "limit": 100,
        "filters": {
            "award_type_codes": ["A","B","C","D"],
            "time_period": [
                {"start_date": start_date, "end_date": end_date}
            ]
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Funding Agency",
            "Funding Sub Agency",
            "Start Date",
            "End Date"
        ]
    }
    resp = requests.post(url, json=body)
    resp.raise_for_status()
    return resp.json()

def iter_usaspending(start_date: str, end_date: str):
    page = 1
    while True:
        data = get_usaspending_page(page, start_date, end_date)
        results = data.get("results", [])
        if not results:
            break
        for r in results:
            yield r
        page += 1

def transform_for_pbi(record):
    return {
        "AwardId": record.get("Award ID"),
        "RecipientName": record.get("Recipient Name"),
        "AwardAmount": record.get("Award Amount"),
        "AwardingAgency": record.get("Awarding Agency"),
        "AwardingSubAgency": record.get("Awarding Sub Agency"),
        "FundingAgency": record.get("Funding Agency"),
        "FundingSubAgency": record.get("Funding Sub Agency"),
        "StartDate": record.get("Start Date"),
        "EndDate": record.get("End Date")
    }

def push_batch_to_powerbi(access_token: str, rows):
    if not rows:
        return
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{PBI_DATASET_ID}/tables/{PBI_TABLE_NAME}/rows"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {"rows": rows}
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()

def main():
    # Example: pull data for current federal fiscal year
    today = date.today()
    fy_start = date(today.year if today.month >= 10 else today.year - 1, 10, 1)
    start_date = fy_start.isoformat()
    end_date = today.isoformat()

    token = get_access_token()

    batch = []
    batch_size = 500  # Power BI allows up to 10k rows per call, keep it modest

    for rec in iter_usaspending(start_date, end_date):
        batch.append(transform_for_pbi(rec))
        if len(batch) >= batch_size:
            push_batch_to_powerbi(token, batch)
            batch = []

    # send remaining
    if batch:
        push_batch_to_powerbi(token, batch)

if __name__ == "__main__":
    main()
