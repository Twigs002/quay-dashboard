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
import os, re, json, time, requests, datetime
from datetime import timezone, timedelta
import pytz

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOCALE       = "en_US"
TIMEZONE     = pytz.timezone("Africa/Johannesburg")
API_BASE     = "https://api.dialfire.com"

BENCHMARKS = {
    "cph":             45,
    "daily_calls":     315,
    "rm_success_rate": 17,
    "fc_success_rate": 20,
}

SELLER_STATUSES = {"LEAD"}
RENTAL_STATUSES = {"RENTAL_LEAD"}
EMAIL_STATUSES  = {"GOT_EMAIL"}

# Agents who ONLY work these campaigns are classified as "RM" (relationship
# manager). Anyone working ClientHub plus another campaign is "Fancy" (Fancy
# Caller).
RM_CAMPAIGNS = {"Clienthub Master", "New Contacts", "No Answer / Not contacted", "CLIENTHUB"}


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


def dates_to_timespan(date_from, date_to):
    """Convert absolute dates to Dialfire 'X-Yday' relative timespan.

    Dialfire timespan 'X-Yday' = from X days ago to Y days ago (UTC).
    We subtract 1 from the end so the full end day is included.
    """
    today = datetime.datetime.now(timezone.utc).date()
    days_from = (today - date_from).days
    days_to   = (today - date_to).days - 1
    if days_to < 0:
        days_to = 0
    if days_from < days_to:
        days_from = days_to
    return f"{days_from}-{days_to}day"


# ---------------------------------------------------------------------------
# HTTP helper with 202-polling
# ---------------------------------------------------------------------------
def fetch_json(url, params, label, tag, max_poll=10):
    """GET url with params; handle DialFire's 202-then-poll async pattern."""
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 202:
            loc = r.headers.get("Location") or r.headers.get("location")
            if not loc:
                try:
                    body = r.json()
                    loc = body.get("url") or body.get("statusUrl") or body.get("location")
                except Exception:
                    pass
            if loc:
                for _ in range(max_poll):
                    time.sleep(3)
                    r2 = requests.get(loc, timeout=30)
                    if r2.status_code == 200:
                        try:    return r2.json()
                        except Exception as e:
                            print(f"  [{label}] {tag} -> poll JSON parse error: {e}")
                            return {}
                    if r2.status_code in (401, 403):
                        print(f"  [{label}] {tag} -> poll {r2.status_code}")
                        return None
                print(f"  [{label}] {tag} -> polling timed out")
                return {}
            else:
                print(f"  [{label}] {tag} -> 202 no poll URL, retrying same URL")
                for _ in range(max_poll):
                    time.sleep(5)
                    r2 = requests.get(url, params=params, timeout=30)
                    if r2.status_code == 200:
                        try:    return r2.json()
                        except Exception as e:
                            print(f"  [{label}] {tag} -> retry JSON parse error: {e}")
                            return {}
                    if r2.status_code in (401, 403):
                        return None
                    if r2.status_code != 202:
                        break
                return {}
        if r.status_code in (401, 403):
            print(f"  [{label}] {tag} -> HTTP {r.status_code} (token issue)")
            return None
        if r.status_code == 200:
            try:    return r.json()
            except Exception as e:
                print(f"  [{label}] {tag} -> JSON parse error: {e}")
                return {}
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return {}
    except Exception as e:
        print(f"  [{label}] {tag} -> error: {e}")
        return {}


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


def fetch_lead_counts(cid, token, ts, label):
    """Lead-status counts per agent for the campaign (editsDef_v2 grouped)."""
    result   = {}
    base_url = f"{API_BASE}/api/campaigns/{cid}/reports/editsDef_v2/report/{LOCALE}"

    params = {
        "access_token": token,
        "asTree":       "true",
        "timespan":     ts,
        "group0":       "Lead_Status",
        "group1":       "user",
        "column0":      "completed",
    }
    data = fetch_json(base_url, params, label, "leads: Lead_Status>user")
    if not (data and isinstance(data, dict)):
        return result

    for sgrp in data.get("groups", []):
        if not isinstance(sgrp, dict):
            continue
        status_val = str(sgrp.get("value", "")).strip().upper()
        bucket = None
        if   status_val in {s.upper() for s in SELLER_STATUSES}: bucket = "seller"
        elif status_val in {s.upper() for s in RENTAL_STATUSES}: bucket = "rental"
        elif status_val in {s.upper() for s in EMAIL_STATUSES}:  bucket = "email"
        if bucket is None:
            continue
        for u in sgrp.get("groups", sgrp.get("children", [])):
            if not isinstance(u, dict):
                continue
            ag = str(u.get("value", "")).strip()
            if not ag or ag in ("-", ""):
                continue
            ucols = u.get("columns", [])
            cnt = 0
            if ucols:
                try:
                    cnt = int(ucols[0]) if ucols[0] not in (None, "", "-") else 0
                except Exception:
                    pass
            if ag not in result:
                result[ag] = {"seller": 0, "rental": 0, "email": 0}
            result[ag][bucket] += cnt
    return result


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


def _norm_camp(n):
    """Strip CM/NA suffix variants. 'Goal Diggers - CM' -> 'Goal Diggers'."""
    return re.sub(r"\s*[_\-\s]*(CM|NA)\s*$", "", n, flags=re.IGNORECASE).strip()


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------
def parse_row(row):
    """Convert one DialFire 'group' row into our agent dict format."""
    name = str(
        row.get("value") or row.get("name") or row.get("user") or
        row.get("username") or row.get("agent_name") or "Unknown"
    ).strip()
    if not name or name in ("-", "—", "–", "Unknown", "None", ""):
        return None

    # editsDef_v2 returns columns positionally in the order we requested:
    # [completed, success, successRate, workTime].
    cols = row.get("columns", [])
    def _col(i, default=0):
        try:    return float(cols[i] or 0)
        except Exception:  return float(default)

    calls   = int(row.get("completed") or row.get("calls") or _col(0) or 0)
    success = int(row.get("success") or _col(1) or 0)
    wt_raw  = float(row.get("workTime") or _col(3) or 0)
    # workTime from editsDef_v2 is in hours unless the raw integer is > 1000,
    # in which case it's milliseconds.
    work_hrs = wt_raw / 3600000 if wt_raw > 1000 else wt_raw

    cph = round(calls / work_hrs, 1) if work_hrs > 0 else 0.0
    sr  = round(success / calls * 100, 1) if calls > 0 else 0.0

    return {
        "name":        name,
        "calls":       calls,
        "success":     success,
        "seller":      int(row.get("seller_lead") or row.get("seller") or 0),
        "rental":      int(row.get("rental_lead") or row.get("rental") or 0),
        "email":       int(row.get("got_email")   or row.get("email")  or 0),
        "cph":         cph,
        "successRate": sr,
        "workTime":    round(work_hrs, 4),
        "is_rm":       False,
        "meetsTarget": False,
        "campaigns":   [],
    }


def merge_agent_row(agents, parsed, cname):
    """Add `parsed` (one campaign's row for an agent) into the running
    `agents` dict.

    For every (agent, campaign) pair we ADD the campaign's counts to the
    agent's running totals -- and append the campaign name. The previous
    implementations in this script and in backfill_dialfire.py had a bug
    where new-campaign rows only appended the name but skipped the counts,
    so multi-campaign agents only ever reflected their first campaign.
    """
    n = parsed["name"]
    if n not in agents:
        # First time we've seen this agent - take parsed as the starting
        # values and start a fresh campaigns list.
        a = parsed.copy()
        a["campaigns"] = [cname] if cname else []
        agents[n] = a
        return

    a = agents[n]
    a["calls"]    += parsed["calls"]
    a["success"]  += parsed["success"]
    a["seller"]   += parsed["seller"]
    a["rental"]   += parsed["rental"]
    a["email"]    += parsed["email"]
    a["workTime"]  = round(a["workTime"] + parsed["workTime"], 4)
    if cname and cname not in a["campaigns"]:
        a["campaigns"].append(cname)


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
# Classification + final stats
# ---------------------------------------------------------------------------
def finalize(agents):
    """Compute cph, successRate, RM/Fancy classification, meetsTarget."""
    for a in agents.values():
        a["cph"] = round(a["calls"] / a["workTime"], 1) if a["workTime"] > 0 else 0.0
        a["successRate"] = round(a["success"] / a["calls"] * 100, 1) if a["calls"] > 0 else 0.0

        camps = set(a.get("campaigns", []))
        a["is_rm"] = bool(camps) and camps.issubset(RM_CAMPAIGNS)

        bench = BENCHMARKS["rm_success_rate"] if a["is_rm"] else BENCHMARKS["fc_success_rate"]
        a["meetsTarget"] = (a["cph"] >= BENCHMARKS["cph"] and a["successRate"] >= bench) if a["calls"] > 0 else False


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
