"""
DialFire metadata probe — find the API column keys for talk/wrap/waiting time.

Strategy: enumerate DialFire's reports metadata endpoints. The user has saved
reports called "Wrap up Time" and "Talk Time Proportion Daily" in the UI; their
stored definitions should contain the column-key strings the API expects.
"""
import os, sys, json, requests
from dialfire_common import LOCALE, API_BASE

CID = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
TOK = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()


def hit(path, params=None, raw=False):
    url = f"{API_BASE}{path}"
    p   = {"access_token": TOK}
    if params:
        p.update(params)
    try:
        r = requests.get(url, params=p, timeout=20)
        print(f"\n=== GET {path}  ({r.status_code}) ===")
        if r.status_code != 200:
            print(r.text[:500])
            return None
        try:
            data = r.json()
        except Exception:
            print(r.text[:1000])
            return None
        if raw:
            print(json.dumps(data, indent=2)[:4000])
        return data
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def main():
    if not (CID and TOK):
        print("ERROR: ClientHub creds missing")
        sys.exit(1)

    # 1) Campaign root – sometimes lists report URLs.
    hit(f"/api/campaigns/{CID}", raw=False)

    # 2) Try a couple of likely listing endpoints.
    for path in [
        f"/api/campaigns/{CID}/reports",
        f"/api/campaigns/{CID}/reports/",
        f"/api/campaigns/{CID}/reports/saved",
        f"/api/campaigns/{CID}/reports/list",
        f"/api/campaigns/{CID}/reports/definitions",
        f"/api/campaigns/{CID}/reports/edits/definitions",
        f"/api/campaigns/{CID}/reports/editsDef_v2",
        f"/api/campaigns/{CID}/reports/editsDef_v2/columns",
        f"/api/campaigns/{CID}/reports/editsDef_v2/{LOCALE}",
        f"/api/campaigns/{CID}/reports/editsDef_v2/definition/{LOCALE}",
        f"/api/campaigns/{CID}/reports/editsDef_v2/columns/{LOCALE}",
        f"/api/campaigns/{CID}/reports/editsDef_v2/meta/{LOCALE}",
        f"/api/campaigns/{CID}/reports/editsDef_v2/fields/{LOCALE}",
    ]:
        data = hit(path)
        if data:
            print(json.dumps(data, indent=2)[:3000])

    # 3) Ask editsDef_v2 with NO column params – see if it returns something
    # describing the available column set.
    print("\n\n=== editsDef_v2 report with NO columns ===")
    hit(
        f"/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}",
        params={"timespan": "7-1day", "group0": "user"},
        raw=True,
    )

    # 4) Ask with a deliberately bogus column to see if the error message
    # lists valid options.
    print("\n\n=== editsDef_v2 with bogus column ===")
    hit(
        f"/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}",
        params={"timespan": "7-1day", "group0": "user", "column0": "thisColumnDoesNotExistXYZ"},
        raw=True,
    )

if __name__ == "__main__":
    main()
