"""
DialFire Daily Stats Fetcher
============================
Fetches per-agent stats for each day in [START_DATE, END_DATE] and writes to:
  - data/daily_data.json   (array of {date, rm, fancy} entries, sorted desc)

Uses a 1-day editsDef_v2 timespan per date so the dashboard's Daily Stats
picker returns distinct numbers for each day instead of falling back to the
containing week's totals.

Environment variables:
  CAMPAIGN_*_ID / CAMPAIGN_*_TOKEN  (same as fetch_dialfire.py)
  DIALFIRE_CAMPAIGNS                 (JSON list fallback)
  START_DATE                         e.g. "2026-05-01" -- defaults to 30 days ago SAST
  END_DATE                           e.g. "2026-06-01" -- defaults to yesterday SAST
"""
import os, json, datetime, requests
from datetime import timezone, timedelta
import pytz

from dialfire_common import (
    LOCALE, API_BASE,
    dates_to_timespan, fetch_json, fetch_lead_counts,
    _norm_camp, parse_row, merge_agent_row, finalize,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TIMEZONE = pytz.timezone("Africa/Johannesburg")


# ---------------------------------------------------------------------------
# Campaign helpers (mirrors fetch_dialfire.py)
# ---------------------------------------------------------------------------
def fetch_campaign_name(cid, token):
    """GET /api/campaigns/{cid} to get the human-readable name."""
    url = f"{API_BASE}/api/campaigns/{cid}"
    try:
        r = requests.get(url, params={"access_token": token}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            name = (data.get("name") or data.get("title") or data.get("label") or "").strip()
            if name:
                return name
    except Exception as e:
        print(f"  Warning: could not fetch campaign name for {cid}: {e}")
    return cid


def fetch_campaign_week(campaign, ts):
    """Fetch per-agent editsDef_v2 stats for one campaign for the given timespan."""
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)
    base  = f"{API_BASE}/api/campaigns/{cid}"

    print(f"  [{label}] timespan={ts}")

    params = {
        "access_token": token,
        "asTree":       "true",
        "timespan":     ts,
        "group0":       "user",
        "column0":      "completed",
        "column1":      "success",
        "column2":      "successRate",
        "column3":      "workTime",
        "column4":      "connectTimeDialer",
        "column5":      "wrapupTime",
        "column6":      "pauseTime",
        "column7":      "waitTimeDialer",
    }

    data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params, label, f"editsDef_v2 ts={ts}")
    if data is None:
        print(f"  [{label}] HTTP 4xx - skipping campaign")
        return []
    if not data:
        print(f"  [{label}] no data")
        return []

    grp = data.get("groups", [])
    if not (isinstance(grp, list) and len(grp) > 0):
        print(f"  [{label}] empty groups")
        return []

    print(f"  [{label}] {len(grp)} agent rows")

    lead_counts = fetch_lead_counts(cid, token, ts, label)
    for item in grp:
        if isinstance(item, dict):
            ag = str(item.get("value", "")).strip()
            if ag in lead_counts:
                item["seller"] = lead_counts[ag]["seller"]
                item["rental"] = lead_counts[ag]["rental"]
                item["email"]  = lead_counts[ag]["email"]
    return grp


# ---------------------------------------------------------------------------
# Campaign configuration (env-var driven, same as fetch_dialfire.py)
# ---------------------------------------------------------------------------
def load_campaigns():
    """Build the list of (id, token, name) campaign tuples from env vars."""
    campaigns = []

    def add(env_id, env_tok, default_label):
        cid = os.environ.get(env_id, "").strip()
        tok = os.environ.get(env_tok, "").strip()
        if cid and tok:
            name = fetch_campaign_name(cid, tok) or default_label
            campaigns.append({"id": cid, "token": tok, "name": name})
            print(f"  Campaign: {default_label} -> {cid} ({name})")
        elif cid:
            print(f"  Campaign: {default_label} -> {cid} (NO TOKEN, skipping)")

    add("CAMPAIGN_CLIENTHUB_ID",           "CAMPAIGN_CLIENTHUB_TOKEN",           "CLIENTHUB")
    add("CAMPAIGN_CLIENTHUB_NEW_ID",       "CAMPAIGN_CLIENTHUB_NEW_TOKEN",       "CLIENTHUB_NEW")
    add("CAMPAIGN_CLIENTHUB_NO_ANSWER_ID", "CAMPAIGN_CLIENTHUB_NO_ANSWER_TOKEN", "CLIENTHUB_NO_ANSWER")

    i = 1
    while True:
        if not os.environ.get(f"CAMPAIGN_{i}_ID", "").strip():
            break
        add(f"CAMPAIGN_{i}_ID", f"CAMPAIGN_{i}_TOKEN", f"CAMP{i}")
        i += 1

    add("ASSASSINS_CM_ID", "ASSASSINS_CM_TOKEN", "ASSASSINS_CM")
    add("ASSASSINS_NA_ID", "ASSASSINS_NA_TOKEN", "ASSASSINS_NA")
    add("AMIGOS_CM_ID",    "AMIGOS_CM_TOKEN",    "AMIGOS_CM")
    add("AMIGOS_NA_ID",    "AMIGOS_NA_TOKEN",    "AMIGOS_NA")

    # Legacy single-campaign env var
    leg_id  = os.environ.get("DIALFIRE_CAMPAIGN_ID", "").strip()
    leg_tok = os.environ.get("DIALFIRE_CAMPAIGN_TOKEN", "").strip()
    if leg_id and leg_tok and not any(c["id"] == leg_id for c in campaigns):
        name = fetch_campaign_name(leg_id, leg_tok) or "LEGACY"
        campaigns.append({"id": leg_id, "token": leg_tok, "name": name})
        print(f"  Campaign: LEGACY -> {leg_id} ({name})")

    # JSON list fallback
    raw = os.environ.get("DIALFIRE_CAMPAIGNS", "")
    if raw:
        try:
            for c in json.loads(raw):
                if c.get("id") and c.get("token") and not any(x["id"] == c["id"] for x in campaigns):
                    if not c.get("name"):
                        c["name"] = fetch_campaign_name(c["id"], c["token"]) or c["id"]
                    campaigns.append(c)
                    print(f"  Campaign: JSON -> {c['id']} ({c['name']})")
        except json.JSONDecodeError as e:
            print(f"  Warning: could not parse DIALFIRE_CAMPAIGNS: {e}")

    return campaigns


# ---------------------------------------------------------------------------
# Date range
# ---------------------------------------------------------------------------
def get_date_range(now_sast):
    """Return list of dates from START_DATE to END_DATE (inclusive).

    Defaults: END_DATE = yesterday SAST, START_DATE = 30 days before END_DATE.
    """
    yesterday = now_sast.date() - timedelta(days=1)
    end_str   = (os.environ.get("END_DATE") or "").strip() or str(yesterday)
    end       = datetime.datetime.strptime(end_str, "%Y-%m-%d").date()

    start_str = (os.environ.get("START_DATE") or "").strip() or str(end - timedelta(days=30))
    start     = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()

    if start > end:
        start = end

    dates = []
    d = start
    while d <= end:
        dates.append(d)
        d += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)
    dates    = get_date_range(now_sast)

    print(f"=== DialFire Daily Fetch ===")
    print(f"Range: {dates[0]} -> {dates[-1]} ({len(dates)} days)")

    campaigns = load_campaigns()
    if not campaigns:
        print("ERROR: no campaigns configured.")
        return

    # ---- Load existing daily_data.json (array form) ----
    daily_path = "data/daily_data.json"
    try:
        with open(daily_path) as f:
            existing = json.load(f)
        if isinstance(existing, dict):
            existing = [{"date": k, **v} for k, v in existing.items()]
        if not isinstance(existing, list):
            existing = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    fetched_dates = set()
    for d in dates:
        ts = dates_to_timespan(d, d)
        print(f"\n--- {d} | timespan={ts} ---")

        agents = {}
        for campaign in campaigns:
            rows = fetch_campaign_week(campaign, ts)
            cname = _norm_camp(campaign.get("name", "")) or campaign.get("name", "")
            for row in rows:
                parsed = parse_row(row)
                if parsed is None:
                    continue
                merge_agent_row(agents, parsed, cname)

        finalize(agents)

        rm_agents    = sorted([a for a in agents.values() if a["is_rm"]],     key=lambda x: -x["calls"])
        fancy_agents = sorted([a for a in agents.values() if not a["is_rm"]], key=lambda x: -x["calls"])

        print(f"  {d}: {len(agents)} agents | RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")

        date_str = str(d)
        existing = [e for e in existing if e.get("date") != date_str]
        existing.append({
            "date":      date_str,
            "generated": now_utc.isoformat(),
            "rm":        rm_agents,
            "fancy":     fancy_agents,
        })
        fetched_dates.add(date_str)

    # Sort by date desc
    existing.sort(key=lambda e: e.get("date", ""), reverse=True)

    os.makedirs("data", exist_ok=True)
    with open(daily_path, "w") as f:
        json.dump(existing, f, indent=2)
    print(f"\nWrote {daily_path} -- {len(existing)} day entries ({len(fetched_dates)} fetched this run)")


if __name__ == "__main__":
    main()
