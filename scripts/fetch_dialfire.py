"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

Timespan format (discovered via testing):
  "0-0day"  = today only
  "7-0day"  = last 7 days  (positive N, no minus sign)
  "14-0day" = last 14 days
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE    = "en_US"
DAYS_BACK = 7
TIMEZONE  = pytz.timezone("Africa/Johannesburg")
API_BASE  = "https://api.dialfire.com"

BENCHMARKS = {
    "cph":            45,
    "daily_calls":    315,
    "rm_success_rate":  17,
    "fc_success_rate":  20,
}

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}


# -- Poll helper --------------------------------------------------------------
def fetch_json(url, params, label, tag, max_polls=8, poll_interval=3):
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 403:
            print(f"  [{label}] {tag} -> HTTP 403 (skip)")
            return None
        if r.status_code == 404:
            print(f"  [{label}] {tag} -> HTTP 404 (skip)")
            return None
        if r.status_code == 202:
            print(f"  [{label}] {tag} -> HTTP 202 (polling...)")
            for _ in range(max_polls):
                time.sleep(poll_interval)
                r2 = requests.get(url, params=params, timeout=30)
                if r2.status_code == 200:
                    print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                    try:
                        return r2.json()
                    except Exception as e:
                        print(f"  [{label}] JSON error after poll: {e}")
                        return []
                if r2.status_code == 403:
                    return None
                if r2.status_code not in (202, 200):
                    return []
            print(f"  [{label}] {tag} -> timed out")
            return []
        if r.status_code == 200:
            print(f"  [{label}] {tag} -> HTTP 200")
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON error: {e} | body={r.text[:200]}")
                return []
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# -- Helpers ------------------------------------------------------------------
def _safe_int(v):
    try:
        return int(float(v)) if v not in (None, "", "--") else 0
    except (TypeError, ValueError):
        return 0

def _safe_float(v, default=0.0):
    try:
        return float(v) if v not in (None, "", "--") else default
    except (TypeError, ValueError):
        return default

def _get_col(stats, col_map, *names, default=0):
    for name in names:
        mapped = col_map.get(name)
        if mapped and mapped in stats:
            return stats[mapped]
        if name in stats:
            return stats[name]
    return default

def _col_names(defs):
    result = []
    for item in defs:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


# -- Parse asTree JSON into rows ----------------------------------------------
def extract_rows(data, label):
    rows = []
    if not isinstance(data, dict):
        print(f"  [{label}] DIAG: not a dict ({type(data).__name__})")
        return rows

    col_defs   = data.get("columnDefs", [])
    grp_defs   = data.get("groupDefs", [])
    groups_raw = data.get("groups", data.get("children", {}))

    col_names  = _col_names(col_defs)
    grp_names  = _col_names(grp_defs)
    col_map    = {n: f"col{i}" for i, n in enumerate(col_names)}
    grp_len    = len(groups_raw) if hasattr(groups_raw, "__len__") else "?"

    print(f"  [{label}] DIAG grpDefs={grp_names} cols={col_names} groups={type(groups_raw).__name__}[{grp_len}]")
    if isinstance(groups_raw, dict) and groups_raw:
        for k in list(groups_raw.keys())[:2]:
            print(f"  [{label}] DIAG groups[{repr(k)}]={json.dumps(groups_raw[k])[:250]}")
    elif isinstance(groups_raw, list) and groups_raw:
        print(f"  [{label}] DIAG groups[0]={json.dumps(groups_raw[0])[:250]}")

    # Normalise to dict
    if isinstance(groups_raw, list):
        if not groups_raw:
            return rows
        groups = {}
        for item in groups_raw:
            if isinstance(item, dict):
                key = (item.get("user") or item.get("name") or
                       item.get(grp_names[0] if grp_names else "user", ""))
                if key and str(key) not in ("total", "--", ""):
                    groups[str(key)] = item
    elif isinstance(groups_raw, dict):
        groups = groups_raw
    else:
        return rows

    if not groups:
        return rows

    # Detect nested date->user structure
    sample    = [k for k in list(groups.keys())[:3] if k not in ("total", "--", "")]
    date_re   = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    is_nested = bool(sample) and all(date_re.match(str(k)) for k in sample) and len(grp_names) > 1

    if is_nested:
        agg = {}
        for date_key, node in groups.items():
            if date_key in ("total", "--", "") or not isinstance(node, dict):
                continue
            inner_raw = node.get("groups", {})
            if isinstance(inner_raw, list):
                inner = {}
                for item in inner_raw:
                    if isinstance(item, dict):
                        uk = (item.get("user") or item.get("name") or
                              item.get(grp_names[-1] if grp_names else "user", ""))
                        if uk:
                            inner[str(uk)] = item
            else:
                inner = inner_raw if isinstance(inner_raw, dict) else {}

            inner_cols  = _col_names(node.get("columnDefs", col_defs))
            inner_map   = {n: f"col{i}" for i, n in enumerate(inner_cols)}

            for uk, stats in inner.items():
                if uk in ("total", "--", "") or not isinstance(stats, dict):
                    continue
                if uk not in agg:
                    agg[uk] = {"name": uk, "completed": 0, "success": 0, "workTime": 0, "declines": 0}
                a = agg[uk]
                a["completed"] += _safe_int(_get_col(stats, inner_map, "completed", "count"))
                a["success"]   += _safe_int(_get_col(stats, inner_map, "success", "connects"))
                a["workTime"]  += _safe_int(_get_col(stats, inner_map, "workTime"))
                a["declines"]  += (
                    _safe_int(_get_col(stats, inner_map, "norespons", "noResponse")) +
                    _safe_int(_get_col(stats, inner_map, "answeringmachines", "answeringMachines"))
                )
        for a in agg.values():
            a["successRate"] = round(a["success"] / a["completed"] * 100, 1) if a["completed"] > 0 else 0.0
        rows = list(agg.values())
    else:
        for uk, stats in groups.items():
            if uk in ("total", "--", "") or not isinstance(stats, dict):
                continue
            completed = _safe_int(_get_col(stats, col_map, "completed", "count"))
            success   = _safe_int(_get_col(stats, col_map, "success", "connects"))
            worktime  = _safe_int(_get_col(stats, col_map, "workTime"))
            norespons = _safe_int(_get_col(stats, col_map, "norespons", "noResponse"))
            answering = _safe_int(_get_col(stats, col_map, "answeringmachines", "answeringMachines"))
            sr        = _safe_float(_get_col(stats, col_map, "successRate", "connectRate", "success_rate"))
            rows.append({"name": uk, "completed": completed, "success": success,
                         "workTime": worktime, "declines": norespons + answering, "successRate": sr})

    print(f"  [{label}] extracted {len(rows)} rows")
    return rows


# -- Parse row into dashboard agent -------------------------------------------
def parse_row(row):
    if not isinstance(row, dict):
        return None
    name = str(row.get("name", row.get("user", ""))).strip()
    if not name or name.lower() in ("total", "--", "grand total"):
        return None
    calls     = _safe_int(row.get("completed", row.get("count", row.get("calls", 0))))
    success   = _safe_int(row.get("success", row.get("connects", 0)))
    declines  = _safe_int(row.get("declines", 0))
    work_secs = _safe_int(row.get("workTime", 0))
    if calls == 0:
        return None
    work_hrs = work_secs / 3600.0
    cph      = round(calls / work_hrs, 1) if work_hrs > 0 else 0.0
    sr = row.get("successRate")
    sr = (round(success / calls * 100, 1) if calls > 0 else 0.0) if (sr is None or sr == "") else _safe_float(sr)
    return {
        "name": name, "calls": calls, "success": success, "declines": declines,
        "cph": cph, "successRate": sr, "workHours": round(work_hrs, 2),
        "meetsTarget": cph >= BENCHMARKS["cph"],
    }


# -- Fetch one campaign -------------------------------------------------------
def fetch_campaign(cid, token, index, total):
    label = f"{index + 1}/{total} {cid}"
    base  = f"{API_BASE}/api/campaigns/{cid}"

    # Try timespans: "N-0day" format = last N days in Dialfire
    timespans = ["0-0day", f"{DAYS_BACK}-0day", "14-0day", "7-0day", "30-0day"]

    for ts in timespans:
        params = {"access_token": token, "asTree": "true", "timespan": ts,
                  "group0": "user", "column0": "completed", "column1": "success",
                  "column2": "successRate", "column3": "workTime"}
        data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params,
                          label, f"editsDef_v2 ts={ts}")
        if data is None:
            print(f"  [{label}] 403 - token invalid, skipping campaign")
            return []
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, "__len__") else 0
            print(f"  [{label}] ts={ts} groups={type(grp).__name__}[{grp_len}]")
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS with ts={ts}")
                    return rows

    # Try dialerStat as fallback
    for ts in timespans:
        params = {"access_token": token, "asTree": "true", "timespan": ts,
                  "group0": "user", "column0": "count", "column1": "connects",
                  "column2": "answeringmachines", "column3": "norespons", "column4": "connectRate"}
        data = fetch_json(f"{base}/reports/dialerStat/report/{LOCALE}", params,
                          label, f"dialerStat ts={ts}")
        if data is None:
            return []
        if isinstance(data, dict):
            grp = data.get("groups", [])
            grp_len = len(grp) if hasattr(grp, "__len__") else 0
            print(f"  [{label}] dialerStat ts={ts} groups={type(grp).__name__}[{grp_len}]")
            if grp_len > 0:
                rows = extract_rows(data, label)
                if rows:
                    print(f"  [{label}] SUCCESS dialerStat ts={ts}")
                    return rows

    print(f"  [{label}] No data found across all timespans")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)
    period_end   = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end}  ({DAYS_BACK} days)")

    # Collect campaigns: CAMPAIGN_n_ID/TOKEN + CAMPAIGN_CLIENTHUB_ID/TOKEN
    campaigns = []

    # Named clienthub campaign (added by user)
    ch_id  = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
    ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
    if ch_id and ch_tok:
        campaigns.append({"id": ch_id, "token": ch_tok, "label": "CLIENTHUB"})
        print(f"  CLIENTHUB campaign: {ch_id}")
    elif ch_id:
        print(f"  CLIENTHUB campaign: {ch_id} (NO TOKEN)")

    # Numbered campaigns
    i = 1
    while True:
        cid  = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            campaigns.append({"id": cid, "token": ctok, "label": f"CAMP{i}"})
            print(f"  Campaign {i}: {cid}")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN - skipping)")
        i += 1

    if not campaigns:
        print("No campaigns configured.")
        return

    print(f"Total campaigns: {len(campaigns)}")
    print()

    all_rows = []
    for idx, c in enumerate(campaigns):
        rows = fetch_campaign(c["id"], c["token"], idx, len(campaigns))
        all_rows.extend(rows)

    print()
    print(f"Raw rows collected: {len(all_rows)}")

    merged = {}
    for row in all_rows:
        agent = parse_row(row)
        if agent is None:
            continue
        name = agent["name"]
        if name in merged:
            ex = merged[name]
            ex["calls"]      += agent["calls"]
            ex["success"]    += agent["success"]
            ex["declines"]   += agent["declines"]
            ex["workHours"]   = round(ex["workHours"] + agent["workHours"], 2)
            ex["cph"]         = round(ex["calls"] / ex["workHours"], 1) if ex["workHours"] > 0 else 0.0
            ex["successRate"] = round(ex["success"] / ex["calls"] * 100, 1) if ex["calls"] > 0 else 0.0
            ex["meetsTarget"] = ex["cph"] >= BENCHMARKS["cph"]
        else:
            merged[name] = agent

    print(f"Unique agents: {len(merged)}")

    rm_agents, fancy_agents = [], []
    for name, agent in sorted(merged.items()):
        (rm_agents if name in RM_NAMES else fancy_agents).append(agent)

    rm_agents.sort(key=lambda x: x["calls"], reverse=True)
    fancy_agents.sort(key=lambda x: x["calls"], reverse=True)

    print(f"RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents:
        print(f"  RM    {a['name']:25s} calls={a['calls']:4d} success={a['success']:3d} declines={a['declines']:3d} cph={a['cph']:5.1f}")
    for a in fancy_agents[:15]:
        print(f"  FANCY {a['name']:25s} calls={a['calls']:4d} success={a['success']:3d} declines={a['declines']:3d} cph={a['cph']:5.1f}")

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    weekly = {"generated": now_utc.isoformat(), "periodStart": str(period_start),
              "periodEnd": str(period_end), "rm": rm_agents, "fancy": fancy_agents}
    with open(os.path.join(data_dir, "weekly_data.json"), "w") as f:
        json.dump(weekly, f, indent=2)
    print(f"Saved weekly_data.json (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    history_path = os.path.join(data_dir, "history.json")
    try:
        with open(history_path) as f:
            raw = json.load(f)
    except Exception:
        raw = []
    history = [h for h in raw if isinstance(h, dict) and "weekStart" in h]
    week_key = str(period_start)
    history  = [h for h in history if h.get("weekStart") != week_key]
    history.append({"weekStart": week_key, "weekEnd": str(period_end),
                    "generated": now_utc.isoformat(), "rm": rm_agents, "fancy": fancy_agents})
    history.sort(key=lambda x: x["weekStart"])
    history = history[-12:]
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json ({len(history)} weeks)")


if __name__ == "__main__":
    main()
