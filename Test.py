import json
import requests
from datetime import date
from pprint import pprint
import csv

# ----------------- CONFIG -----------------

# Short date range for debugging – keep it small first
START_DATE = "2024-10-01"
END_DATE   = "2024-10-07"

# Contract awards only (A–D). Adjust as needed.
AWARD_TYPE_CODES = ["A", "B", "C", "D"]

# Fields we care about (must match USAspending docs)
FIELDS = [
    "Award ID",
    "Recipient Name",
    "Award Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Funding Agency",
    "Funding Sub Agency",
    "Start Date",
    "End Date",
    "Recipient State",
    "Recipient City",
    "Recipient Location",
]

# Max pages to pull while debugging (each page = 100 rows)
MAX_PAGES = 3


# ----------------- CORE FUNCTIONS -----------------

def build_request_body(page: int) -> dict:
    """
    Build the JSON request body for USAspending.
    This is the exact structure you'd copy into the
    production script that pushes to Power BI.
    """
    body = {
        "subawards": False,
        "page": page,
        "limit": 100,
        "filters": {
            "award_type_codes": AWARD_TYPE_CODES,
            "time_period": [
                {
                    "start_date": START_DATE,
                    "end_date": END_DATE
                }
            ]
        },
        "fields": FIELDS
    }
    return body


def get_usaspending_page(page: int) -> dict:
    url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

    body = build_request_body(page)

    print(f"\n=== REQUEST PAGE {page} ===")
    print("URL:", url)
    print("JSON body being sent:")
    print(json.dumps(body, indent=2))

    resp = requests.post(
        url,
        json=body,
        verify=False  # <<< TURN OFF SSL VERIFICATION (debug only)
    )
    print("Status code:", resp.status_code)

    if not resp.ok:
        print("Error response text:")
        print(resp.text)
        resp.raise_for_status()

    return resp.json()

def transform_for_powerbi(record: dict) -> dict:
    """
    Shape one USAspending record into the row structure
    you'd push to Power BI (push dataset).
    """
    loc = record.get("Recipient Location") or {}
    return {
        "AwardId":          record.get("Award ID"),
        "RecipientName":    record.get("Recipient Name"),
        "AwardAmount":      record.get("Award Amount"),
        "AwardingAgency":   record.get("Awarding Agency"),
        "AwardingSubAgency":record.get("Awarding Sub Agency"),
        "FundingAgency":    record.get("Funding Agency"),
        "FundingSubAgency": record.get("Funding Sub Agency"),
        "StartDate":        record.get("Start Date"),
        "EndDate":          record.get("End Date"),     
        "Recipient State": loc.get("state_name"),
        "Recipient City":   loc.get("city_name"),
        "Recipient Country": loc.get("country_name"),
    }


def fetch_all_debug():
    """
    Pull a few pages, print samples, and write a CSV locally.
    """
    all_raw = []
    all_transformed = []

    for page in range(1, MAX_PAGES + 1):
        data = get_usaspending_page(page)
        results = data.get("results", [])

        print(f"Page {page} returned {len(results)} records")

        if not results:
            break

        # Save raw + transformed
        all_raw.extend(results)
        all_transformed.extend(transform_for_powerbi(r) for r in results)

        # Stop early if API tells you there are no more pages
        page_meta = data.get("page_metadata", {})
        if not page_meta.get("hasNext", False):
            print("No more pages according to page_metadata.hasNext")
            break

    # Show a couple of raw records
    print("\n=== SAMPLE RAW RECORD ===")
    if all_raw:
        pprint(all_raw[0])
    else:
        print("No data returned in the selected date range / filters.")

    # Show a couple of transformed rows
    print("\n=== SAMPLE TRANSFORMED ROW (Power BI schema) ===")
    if all_transformed:
        pprint(all_transformed[0])

    # Write to CSV for inspection
    if all_transformed:
        csv_filename = "usaspending_debug_output.csv"
        fieldnames = list(all_transformed[0].keys())
        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_transformed)

        print(f"\nWrote {len(all_transformed)} rows to {csv_filename}")
    else:
        print("\nNo transformed rows to write to CSV.")


# ----------------- ENTRYPOINT -----------------

if __name__ == "__main__":
    fetch_all_debug()
