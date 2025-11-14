import json
import requests
from pprint import pprint
import csv

# ----------------- CONFIG -----------------

# Short date range for debugging – expand later once you're happy
START_DATE = "2024-10-01"
END_DATE   = "2024-10-07"

# Contract awards only (A–D). Adjust as needed.
AWARD_TYPE_CODES = ["A", "B", "C", "D"]

# Fields we want from spending_by_award.
# These labels are supported by the API (see Power Platform thread & docs).
FIELDS = [
    "Award ID",
    "Award Amount",
    "Start Date",
    "End Date",

    "Awarding Agency",
    "Awarding Agency Code",
    "Awarding Sub Agency",
    "Awarding Sub Agency Code",

    "Recipient Name",
    "recipient_id",

    "Recipient Location"
]

# Max pages to pull while debugging (each page = 100 rows)
MAX_PAGES = 3


# ----------------- CORE FUNCTIONS -----------------

def build_request_body(page: int) -> dict:
    """
    Build the JSON request body for USAspending.
    """
    return {
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
        verify=False  # << debug only, because of your SSL intercept
    )
    print("Status code:", resp.status_code)

    if not resp.ok:
        print("Error response text:")
        print(resp.text)
        resp.raise_for_status()

    return resp.json()


def flatten_location(loc: dict) -> dict:
    """
    Take the Recipient Location object and flatten the parts we care about.
    """
    if not loc:
        return {
            "Address": None,
            "City": None,
            "StateName": None,
            "StateCode": None,
            "Country": None,
        }

    addr_parts = [
        loc.get("address_line1"),
        loc.get("address_line2"),
        loc.get("address_line3"),
    ]
    address = ", ".join([p for p in addr_parts if p])

    return {
        "Address":   address or None,
        "City":      loc.get("city_name"),
        "StateName": loc.get("state_name"),
        "StateCode": loc.get("state_code"),
        "Country":   loc.get("country_name"),
    }


# ----------------- BUILD FACT + DIM TABLES -----------------

def fetch_and_model():
    all_raw = []

    # 1) Pull raw awards
    for page in range(1, MAX_PAGES + 1):
        data = get_usaspending_page(page)
        results = data.get("results", [])

        print(f"Page {page} returned {len(results)} records")

        if not results:
            break

        all_raw.extend(results)

        page_meta = data.get("page_metadata", {})
        if not page_meta.get("hasNext", False):
            print("No more pages according to page_metadata.hasNext")
            break

    if not all_raw:
        print("No data returned for this date range / filters.")
        return

    print("\n=== SAMPLE RAW RECORD KEYS ===")
    pprint(sorted(all_raw[0].keys()))

    # 2) Build FactAwards
    fact_awards = []
    for rec in all_raw:
        fact_awards.append({
            "AwardId":             rec.get("Award ID"),
            "RecipientId":         rec.get("recipient_id"),
            "AwardAmount":         rec.get("Award Amount"),
            "StartDate":           rec.get("Start Date"),
            "EndDate":             rec.get("End Date"),
            "AwardingAgencyCode":  rec.get("Awarding Agency Code"),
            "AwardingSubAgencyCode": rec.get("Awarding Sub Agency Code"),
        })

    # 3) Build DimRecipients (dedupe by recipient_id)
    recipients_by_id = {}
    for rec in all_raw:
        rid = rec.get("recipient_id")
        if not rid:
            continue

        loc = flatten_location(rec.get("Recipient Location") or {})
        # Only create / overwrite with non-null-ish values
        current = recipients_by_id.get(rid, {})
        recipients_by_id[rid] = {
            "RecipientId":   rid,
            "RecipientName": rec.get("Recipient Name") or current.get("RecipientName"),
            "Address":       loc["Address"] or current.get("Address"),
            "City":          loc["City"] or current.get("City"),
            "State":         loc["StateName"] or current.get("State"),
            "Country":       loc["Country"] or current.get("Country"),
        }

    dim_recipients = list(recipients_by_id.values())

    # 4) Build DimSubAgencies (dedupe by subagency code)
    subagencies_by_code = {}
    for rec in all_raw:
        sub_code = rec.get("Awarding Sub Agency Code")
        if not sub_code:
            continue

        sub_name = rec.get("Awarding Sub Agency")
        agency_code = rec.get("Awarding Agency Code")

        if sub_code not in subagencies_by_code:
            subagencies_by_code[sub_code] = {
                "AwardingSubAgencyCode": sub_code,
                "AwardingSubAgencyName": sub_name,
                "AwardingAgencyCode":    agency_code,
            }

    dim_subagencies = list(subagencies_by_code.values())

    # 5) Build DimAgencies (dedupe by awarding agency code)
    agencies_by_code = {}
    for rec in all_raw:
        a_code = rec.get("Awarding Agency Code")
        if not a_code:
            continue

        a_name = rec.get("Awarding Agency")
        if a_code not in agencies_by_code:
            agencies_by_code[a_code] = {
                "AwardingAgencyCode": a_code,
                "AwardingAgencyName": a_name,
            }

    dim_agencies = list(agencies_by_code.values())

    # 6) Write to CSVs
    write_csv("FactAwards.csv", fact_awards)
    write_csv("DimRecipients.csv", dim_recipients)
    write_csv("DimSubAgencies.csv", dim_subagencies)
    write_csv("DimAgencies.csv", dim_agencies)

    print("\n=== SAMPLE FactAwards row ===")
    pprint(fact_awards[0])

    if dim_recipients:
        print("\n=== SAMPLE DimRecipients row ===")
        pprint(dim_recipients[0])


def write_csv(filename: str, rows: list[dict]):
    if not rows:
        print(f"{filename}: no rows to write.")
        return
    fieldnames = list(rows[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {filename}")


# ----------------- ENTRYPOINT -----------------

if __name__ == "__main__":
    fetch_and_model()