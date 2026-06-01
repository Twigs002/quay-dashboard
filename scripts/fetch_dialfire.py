"""
DialFire Weekly Stats Fetcher (Mon-Sun)
========================================
Fetches the current week's per-agent stats from DialFire and writes to:
  - data/weekly_data.json   (latest snapshot, used by the dashboard)
  - data/history.json       (week-by-week history, used by the charts)

Week boundary: Monday 00:00 -> Sunday 23:59 SAST.
On Mondays we fetch the PREVIOUS completed Mon-Sun week (since the new
week has just started and has no meaningful data yet).

Uses the same per-campaign editsDef_v2 endpoint as backfill_dialfire.py.
Aggregates correctly across all campaigns an agent appears in (this was
broken in earlier versions and in backfill_dialfire.py - see the docstring
of merge_agent_row below).
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
# Date helpers
# ---------------------------------------------------------------------------
def get_week_bounds(now_sast):
    """Return (monday, sunday) for the week we should fetch.

    On Mondays we fetch the PREVIOUS completed week (so the dashboard shows
    last week's full Mon-Sun). On Tue-Sun we fetch the CURRENT week (which
    is partial week-to-date).
    """
    today = now_sast.date()
    weekday = today.weekday()  # 0=Mon ... 6=Sun
    if weekday == 0:
        monday = today - timedelta(days=7)
    else:
        monday = today - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


# ---------------------------------------------------------------------------
# Campaign helpers
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
# Campaign configuration (env-var driven)
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
# Main
# ---------------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)
    monday, sunday = get_week_bounds(now_sast)
    ts = dates_to_timespan(monday, sunday)

    print(f"=== DialFire Weekly Fetch ===")
    print(f"Week: {monday} (Mon) -> {sunday} (Sun) | timespan={ts}")

    campaigns = load_campaigns()
    if not campaigns:
        print("ERROR: no campaigns configured.")
        return

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

    print()
    print(f"Unique agents: {len(agents)} | RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents + fancy_agents:
        grp = "RM   " if a["is_rm"] else "FANCY"
        print(f"  {grp} {a['name']:<22} calls={a['calls']:>4} workH={a['workTime']:>7.2f} cph={a['cph']:>5} campaigns={a.get('campaigns')}")

    # ---- weekly_data.json (current snapshot for the dashboard) ----
    week_str = str(monday)
    output = {
        "generated":   now_utc.isoformat(),
        "week":        week_str,
        "weekStart":   week_str,
        "weekEnd":     str(sunday),
        "periodStart": week_str,
        "periodEnd":   str(sunday),
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/weekly_data.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nWrote data/weekly_data.json")

    # ---- history.json (week-by-week archive) ----
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

    # Replace any existing entry for this week, then insert fresh at the top.
    history = [e for e in history if e.get("week") != week_str and e.get("weekStart") != week_str]
    history.insert(0, output)

    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated data/history.json -- {len(history)} weeks total")


if __name__ == "__main__":
    main()
