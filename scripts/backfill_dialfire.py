"""
DialFire Historical Backfill Script
=====================================
Fetches every Mon-Sun week between START_DATE and END_DATE,
and writes each week into history.json.

Uses the same api.dialfire.com + access_token + editsDef_v2 approach as fetch_dialfire.py.
Converts absolute dates to relative timespans (e.g. "36-30day") for the editsDef_v2 endpoint.
Skips weeks that already have real agent data (rm or fancy not empty).

Environment variables:
  CAMPAIGN_CLIENTHUB_ID / CAMPAIGN_CLIENTHUB_TOKEN  (preferred)
  CAMPAIGN_1_ID / CAMPAIGN_1_TOKEN                  (optional extra campaigns)
  CAMPAIGN_2_ID / CAMPAIGN_2_TOKEN                  (optional extra campaigns)
  DIALFIRE_CAMPAIGNS  (fallback JSON list)
  START_DATE          e.g. "2026-03-01" -- required
  END_DATE            e.g. "2026-04-13" -- optional, defaults to yesterday
"""

import os, json
from datetime import datetime, timedelta, timezone

from dialfire_common import (
    LOCALE, API_BASE,
    dates_to_timespan, fetch_json, fetch_lead_counts,
    _norm_camp, parse_row, merge_agent_row, finalize,
)

# ---------------------------------------------------------------------------
# Campaign loading (module-level, same as before)
# ---------------------------------------------------------------------------
CAMPAIGNS = []
ch_id  = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
if ch_id and ch_tok:
    CAMPAIGNS.append({"id": ch_id, "token": ch_tok, "name": "CLIENTHUB"})

ch_new_id  = os.environ.get("CAMPAIGN_CLIENTHUB_NEW_ID", "").strip()
ch_new_tok = os.environ.get("CAMPAIGN_CLIENTHUB_NEW_TOKEN", "").strip()
if ch_new_id and ch_new_tok:
    CAMPAIGNS.append({"id": ch_new_id, "token": ch_new_tok, "name": "CLIENTHUB"})

i = 1
while True:
    cid = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
    tok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
    if not cid or not tok:
        break
    CAMPAIGNS.append({"id": cid, "token": tok, "name": f"CAMP{i}"})
    i += 1

ass_cm_id  = os.environ.get("ASSASSINS_CM_ID", "").strip()
ass_cm_tok = os.environ.get("ASSASSINS_CM_TOKEN", "").strip()
if ass_cm_id and ass_cm_tok:
    CAMPAIGNS.append({"id": ass_cm_id, "token": ass_cm_tok, "name": "ASSASSINS_CM"})

ass_na_id  = os.environ.get("ASSASSINS_NA_ID", "").strip()
ass_na_tok = os.environ.get("ASSASSINS_NA_TOKEN", "").strip()
if ass_na_id and ass_na_tok:
    CAMPAIGNS.append({"id": ass_na_id, "token": ass_na_tok, "name": "ASSASSINS_NA"})
amigos_cm_id  = os.environ.get("AMIGOS_CM_ID", "").strip()
amigos_cm_tok = os.environ.get("AMIGOS_CM_TOKEN", "").strip()
if amigos_cm_id and amigos_cm_tok:
    CAMPAIGNS.append({"id": amigos_cm_id, "token": amigos_cm_tok, "name": "AMIGOS_CM"})

amigos_na_id  = os.environ.get("AMIGOS_NA_ID", "").strip()
amigos_na_tok = os.environ.get("AMIGOS_NA_TOKEN", "").strip()
if amigos_na_id and amigos_na_tok:
    CAMPAIGNS.append({"id": amigos_na_id, "token": amigos_na_tok, "name": "AMIGOS_NA"})

FORCE_REFETCH = os.environ.get("FORCE_REFETCH", "").strip().lower() in ("true", "1", "yes")

# Always append from DIALFIRE_CAMPAIGNS (in addition to any hardcoded vars above)
raw = os.environ.get("DIALFIRE_CAMPAIGNS", "")
if raw:
    try:
        for c in json.loads(raw):
            if c.get("id") and c.get("token") and not any(x["id"] == c["id"] for x in CAMPAIGNS):
                CAMPAIGNS.append(c)
    except Exception as e:
        print(f"Warning: could not parse DIALFIRE_CAMPAIGNS: {e}")

if not CAMPAIGNS:
    raise SystemExit("ERROR: No campaigns configured.")

print(f"Campaigns loaded: {[c['name'] for c in CAMPAIGNS]}")


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------
def get_weeks(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    monday = start - timedelta(days=start.weekday())
    weeks = []
    while monday <= end:
        sunday = monday + timedelta(days=6)
        if sunday > end:
            sunday = end
        weeks.append((monday, sunday))
        monday += timedelta(days=7)
    return weeks


def fetch_campaign_week(campaign, date_from, date_to):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)
    base  = f"{API_BASE}/api/campaigns/{cid}"

    ts = dates_to_timespan(date_from, date_to)
    print(f"  [{label}] timespan={ts} (for {date_from} -> {date_to})")

    # Use editsDef_v2 with relative timespan -- same as daily fetch_dialfire.py
    params = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "user",
        "column0": "completed",
        "column1": "success",
        "column2": "successRate",
        "column3": "workTime",
        "column4": "connectTimeDialer",
        "column5": "wrapupTime",
        "column6": "pauseTime",
        "column7": "waitTimeDialer",
    }

    data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params, label, f"editsDef_v2 ts={ts}")
    if data is None:
        print(f"  [{label}] 403 -- token invalid, skipping campaign")
        return []
    if not data:
        print(f"  [{label}] No data returned")
        return []

    grp = data.get("groups", [])
    if isinstance(grp, list) and len(grp) > 0:
        print(f"  [{label}] editsDef_v2 -> {len(grp)} groups")
        # Fetch lead counts and attach to each group row
        lead_counts = fetch_lead_counts(cid, token, ts, label)
        if lead_counts:
            print(f"  [{label}] lead counts: {lead_counts}")
            for item in grp:
                if isinstance(item, dict):
                    ag_name = str(item.get("value","")).strip()
                    if ag_name in lead_counts:
                        item["seller"] = lead_counts[ag_name]["seller"]
                        item["rental"] = lead_counts[ag_name]["rental"]
                        item["email"]  = lead_counts[ag_name]["email"]
        return grp

    print(f"  [{label}] editsDef_v2 -> empty groups")
    return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    start_date = (os.environ.get("START_DATE") or "").strip()
    if not start_date:
        raise ValueError("START_DATE is required (e.g. 2026-03-01)")

    today_date = datetime.now(timezone.utc).date()
    end_date   = (os.environ.get("END_DATE") or "").strip() or str(today_date - timedelta(days=1))

    weeks = get_weeks(start_date, end_date)
    print(f"\n Backfill range: {start_date} to {end_date}")
    print(f" Weeks to fetch: {len(weeks)}\n")

    hist_path = "data/history.json"
    try:
        with open(hist_path) as f:
            history = json.load(f)
        if isinstance(history, dict):
            history = list(history.values())
        if not isinstance(history, list):
            history = []
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    # Build a set of week keys that have REAL data (non-empty rm or fancy)
    weeks_with_data = set()
    for e in history:
        has_data = (len(e.get("rm", [])) > 0) or (len(e.get("fancy", [])) > 0)
        if has_data:
            if e.get("weekStart"):
                weeks_with_data.add(e["weekStart"])
            if e.get("week"):
                weeks_with_data.add(e["week"])

    print(f" Existing history entries: {len(history)}")
    print(f" Weeks with real data: {len(weeks_with_data)}")

    total_weeks = len(weeks)
    for week_idx, (date_from, date_to) in enumerate(weeks):
        key = str(date_from)
        print(f"\n***{week_idx+1}/{total_weeks}*** Week {date_from} -> {date_to}")

        if key in weeks_with_data and not FORCE_REFETCH:
            print(f"  Already has data -- skipping")
            continue
        elif key in weeks_with_data and FORCE_REFETCH:
            print(f"  Force-refetching (FORCE_REFETCH=true)...")
            history = [e for e in history if e["week"] != key]

        agents = {}
        for campaign in CAMPAIGNS:
            rows = fetch_campaign_week(campaign, date_from, date_to)
            cname = _norm_camp(campaign.get("name", ""))
            for row in rows:
                parsed = parse_row(row)
                if parsed is None:
                    continue
                n = parsed["name"]
                if not n or n in ("Unknown", "-", "\u2014", "\u2013", "None"):
                    continue
                merge_agent_row(agents, parsed, cname)

        finalize(agents)

        rm    = [v for v in agents.values() if v["is_rm"]]
        fancy = [v for v in agents.values() if not v["is_rm"]]

        print(f"  {len(agents)} agents, {sum(v['calls'] for v in agents.values())} calls, {len(rm)} RM, {len(fancy)} Fancy")

        history = [e for e in history if e.get("weekStart") != key and e.get("week") != key]
        history.insert(0, {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "week":      key,
            "weekStart": key,
            "weekEnd":   str(date_to),
            "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
            "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
        })

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)

    print(f"\n{'='*50}")
    print(f"Backfill complete -- {len(history)} weeks in history.json")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
