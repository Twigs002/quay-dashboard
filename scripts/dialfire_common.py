"""
DialFire Common Module
======================
Shared constants, HTTP helpers, row parsing, and agent logic used by both
fetch_dialfire.py (weekly fetch) and backfill_dialfire.py (historical backfill).
"""
import re, json, time, requests, datetime
from datetime import timezone, timedelta

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LOCALE   = "en_US"
API_BASE = "https://api.dialfire.com"

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
RM_CAMPAIGNS = {
    "Clienthub Master",
    "New Contacts",
    "No Answer / Not Contacted",
    "CLIENTHUB",
}


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
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
def _norm_camp(n):
    """Strip CM/NA suffix variants. 'Goal Diggers - CM' -> 'Goal Diggers'."""
    return re.sub(r"\s*[_\-\s]*(CM|NA)\s*$", "", n, flags=re.IGNORECASE).strip()


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


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------
def parse_row(row):
    """Convert one DialFire 'group' row into our agent dict format."""
    name = str(
        row.get("value") or row.get("name") or row.get("user") or
        row.get("username") or row.get("agent_name") or "Unknown"
    ).strip()
    if not name or name in ("-", "\u2014", "\u2013", "Unknown", "None", ""):
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

    talk_hrs  = float(_col(4) or 0)   # connectTimeDialer
    wrap_hrs  = float(_col(5) or 0)   # wrapupTime
    pause_hrs = float(_col(6) or 0)   # pauseTime
    wait_hrs  = float(_col(7) or 0)   # waitTimeDialer

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
        "talkTime":    round(talk_hrs, 4),
        "wrapTime":    round(wrap_hrs, 4),
        "pauseTime":   round(pause_hrs, 4),
        "waitTime":    round(wait_hrs, 4),
        "is_rm":       False,
        "meetsTarget": False,
        "campaigns":   [],
    }


# ---------------------------------------------------------------------------
# Agent aggregation
# ---------------------------------------------------------------------------
def merge_agent_row(agents, parsed, cname):
    """Add `parsed` (one campaign's row for an agent) into the running
    `agents` dict.

    For every (agent, campaign) pair we ADD the campaign's counts to the
    agent's running totals -- and append the campaign name. The previous
    implementations had a bug where new-campaign rows only appended the name
    but skipped the counts, so multi-campaign agents only ever reflected
    their first campaign.
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
    a["workTime"]  = round(a["workTime"]  + parsed["workTime"],  4)
    a["talkTime"]  = round(a.get("talkTime",0)  + parsed.get("talkTime",0),  4)
    a["wrapTime"]  = round(a.get("wrapTime",0)  + parsed.get("wrapTime",0),  4)
    a["pauseTime"] = round(a.get("pauseTime",0) + parsed.get("pauseTime",0), 4)
    a["waitTime"]  = round(a.get("waitTime",0)  + parsed.get("waitTime",0),  4)
    if cname and cname not in a["campaigns"]:
        a["campaigns"].append(cname)


# ---------------------------------------------------------------------------
# Classification + final stats
# ---------------------------------------------------------------------------
def finalize(agents):
    """Compute cph, successRate, RM/Fancy classification, meetsTarget,
    plus the three time-share percentages used by the All Staff page."""
    for a in agents.values():
        a["cph"] = round(a["calls"] / a["workTime"], 1) if a["workTime"] > 0 else 0.0
        a["successRate"] = round(a["success"] / a["calls"] * 100, 1) if a["calls"] > 0 else 0.0

        wt = a.get("workTime", 0) or 0
        a["talkPct"] = round(a.get("talkTime", 0) / wt * 100, 1) if wt > 0 else 0.0
        a["wrapPct"] = round(a.get("wrapTime", 0) / wt * 100, 1) if wt > 0 else 0.0
        a["waitPct"] = round(a.get("waitTime", 0) / wt * 100, 1) if wt > 0 else 0.0
        # "work %" = % of session actively dialling (not paused)
        denom = wt + a.get("pauseTime", 0)
        a["workPct"] = round(wt / denom * 100, 1) if denom > 0 else 0.0

        camps = set(a.get("campaigns", []))
        a["is_rm"] = bool(camps) and camps.issubset(RM_CAMPAIGNS)

        bench = BENCHMARKS["rm_success_rate"] if a["is_rm"] else BENCHMARKS["fc_success_rate"]
        a["meetsTarget"] = (
            a["cph"] >= BENCHMARKS["cph"] and a["successRate"] >= bench
        ) if a["calls"] > 0 else False
