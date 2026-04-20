"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Uses per-campaign tokens to fetch agent statistics.

API endpoint (confirmed by DialFire support):
  GET /api/campaigns/{campaign_id}/reports/{template}/report/{locale}
  ?asTree=true  -> returns JSON
  ?asTree=false -> returns CSV

GitHub Secrets required:
  DIALFIRE_TENANT_ID     - tenant id
  DIALFIRE_TENANT_TOKEN  - tenant-level Bearer token

  Per-campaign:
  CAMPAIGN_1_ID          - campaign id  (e.g. DXX5XQHGZ3R4W6R3)
  CAMPAIGN_1_TOKEN       - campaign-level access token
  CAMPAIGN_2_ID          - campaign id  (e.g. N9EA67VHYX6HZHFG)
  CAMPAIGN_2_TOKEN       - campaign-level access token
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE    = "en_US"
DAYS_BACK = 14
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
def fetch_json_report(url, params, label, tag, max_polls=8, poll_interval=3):
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 403:
            print(f"  [{label}] {tag} -> HTTP 403  (skip)")
            return None
        if r.status_code == 404:
            print(f"  [{label}] {tag} -> HTTP 404  (skip)")
            return None
        if r.status_code == 202:
            print(f"  [{label}] {tag} -> HTTP 202  (async, polling...)")
            for attempt in range(max_polls):
                time.sleep(poll_interval)
                r2 = requests.get(url, params=params, timeout=30)
                if r2.status_code == 200:
                    print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                    try:
                        return r2.json()
                    except Exception as e:
                        print(f"  [{label}] JSON parse error after poll: {e}")
                        return []
                if r2.status_code == 403:
                    return None
                if r2.status_code not in (202, 200):
                    print(f"  [{label}] {tag} -> HTTP {r2.status_code} during poll")
                    return []
            print(f"  [{label}] {tag} -> timed out after {max_polls} polls")
            return []
        if r.status_code == 200:
            print(f"  [{label}] {tag} -> HTTP 200")
            try:
                return r.json()
            except Exception as e:
                print(f"  [{label}] JSON parse error: {e} | body={r.text[:200]}")
                return []
        print(f"  [{label}] {tag} -> HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  [{label}] {tag} -> Exception: {e}")
        return []


# -- Column helpers -----------------------------------------------------------
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


def _build_col_names(col_defs_raw):
    result = []
    for item in col_defs_raw:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("name", ""))
        else:
            result.append(str(item))
    return result


# -- Parse asTree=true JSON into user rows ------------------------------------
def extract_rows_from_tree(data, label):
    rows = []
    if not isinstance(data, dict):
        print(f"  [{label}] DIAG: data is not dict, type={type(data).__name__}")
        return rows

    col_defs_raw   = data.get("columnDefs", [])
    group_defs_raw = data.get("groupDefs", [])
    groups_raw     = data.get("groups", data.get("children", {}))

    col_names   = _build_col_names(col_defs_raw)
    group_names = _build_col_names(group_defs_raw)
    col_map     = {name: f"col{i}" for i, name in enumerate(col_names)}

    print(f"  [{label}] DIAG keys={list(data.keys())}")
    print(f"  [{label}] DIAG groupDefs={group_names}")
    print(f"  [{label}] DIAG colNames={col_names}")
    print(f"  [{label}] DIAG groups type={type(groups_raw).__name__} len={len(groups_raw) if hasattr(groups_raw,'__len__') else '?'}")
    if isinstance(groups_raw, dict) and groups_raw:
        sample_k = list(groups_raw.keys())[:2]
        for k in sample_k:
            print(f"  [{label}] DIAG groups[{k!r}]={json.dumps(groups_raw[k])[:300]}")
    elif isinstance(groups_raw, list) and groups_raw:
        print(f"  [{label}] DIAG groups[0]={json.dumps(groups_raw[0])[:300]}")

    # Normalise groups to a dict
    if isinstance(groups_raw, list):
        if not groups_raw:
            return rows
        groups = {}
        for item in groups_raw:
            if isinstance(item, dict):
                user_key = (item.get("user") or item.get("name") or
                            item.get(group_names[0] if group_names else "user", ""))
                if user_key and str(user_key) not in ("total", "--", ""):
                    groups[str(user_key)] = item
    elif isinstance(groups_raw, dict):
        groups = groups_raw
    else:
        return rows

    if not groups:
        print(f"  [{label}] DIAG: groups empty after normalise")
        return rows

    # Detect nested structure (group0=date, group1=user)
    sample_keys      = [k for k in list(groups.keys())[:3] if k not in ("total", "--", "")]
    date_pattern     = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    looks_like_dates = bool(sample_keys) and all(date_pattern.match(str(k)) for k in sample_keys)

    if looks_like_dates and len(group_names) > 1:
        user_agg = {}
        for date_key, date_node in groups.items():
            if date_key in ("total", "--", "") or not isinstance(date_node, dict):
                continue
            inner_groups_raw = date_node.get("groups", {})
            if isinstance(inner_groups_raw, list):
                inner_groups = {}
                for item in inner_groups_raw:
                    if isinstance(item, dict):
                        uk = (item.get("user") or item.get("name") or
                              item.get(group_names[-1] if group_names else "user", ""))
                        if uk:
                            inner_groups[str(uk)] = item
            else:
                inner_groups = inner_groups_raw if isinstance(inner_groups_raw, dict) else {}

            inner_col_raw = date_node.get("columnDefs", col_defs_raw)
            inner_names   = _build_col_names(inner_col_raw)
            inner_map     = {name: f"col{i}" for i, name in enumerate(inner_names)}

            for user_key, stats in inner_groups.items():
                if user_key in ("total", "--", "") or not isinstance(stats, dict):
                    continue
                if user_key not in user_agg:
                    user_agg[user_key] = {"name": user_key, "completed": 0, "success": 0, "workTime": 0, "declines": 0}
                agg = user_agg[user_key]
                agg["completed"] += _safe_int(_get_col(stats, inner_map, "completed", "count"))
                agg["success"]   += _safe_int(_get_col(stats, inner_map, "success", "connects"))
                agg["workTime"]  += _safe_int(_get_col(stats, inner_map, "workTime"))
                agg["declines"]  += (
                    _safe_int(_get_col(stats, inner_map, "norespons", "noResponse")) +
                    _safe_int(_get_col(stats, inner_map, "answeringmachines", "answeringMachines"))
                )

        for agg in user_agg.values():
            agg["successRate"] = round(agg["success"] / agg["completed"] * 100, 1) if agg["completed"] > 0 else 0.0
        rows = list(user_agg.values())

    else:
        for user_key, stats in groups.items():
            if user_key in ("total", "--", "") or not isinstance(stats, dict):
                continue
            completed = _safe_int(_get_col(stats, col_map, "completed", "count"))
            success   = _safe_int(_get_col(stats, col_map, "success", "connects"))
            work_time = _safe_int(_get_col(stats, col_map, "workTime"))
            norespons = _safe_int(_get_col(stats, col_map, "norespons", "noResponse"))
            answering = _safe_int(_get_col(stats, col_map, "answeringmachines", "answeringMachines"))
            sr        = _safe_float(_get_col(stats, col_map, "successRate", "connectRate", "success_rate"))
            rows.append({
                "name":        user_key,
                "completed":   completed,
                "success":     success,
                "workTime":    work_time,
                "declines":    norespons + answering,
                "successRate": sr,
            })

    if rows:
        print(f"  [{label}] Extracted {len(rows)} user rows")
    else:
        print(f"  [{label}] DIAG: 0 rows from {len(groups)} groups")
    return rows


# -- Parse a raw row into a dashboard agent dict ------------------------------
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
    sr       = row.get("successRate")
    if sr is None or sr == "":
        sr = round(success / calls * 100, 1) if calls > 0 else 0.0
    else:
        sr = _safe_float(sr)

    return {
        "name":        name,
        "calls":       calls,
        "success":     success,
        "declines":    declines,
        "cph":         cph,
        "successRate": sr,
        "workHours":   round(work_hrs, 2),
        "meetsTarget": cph >= BENCHMARKS["cph"],
    }


# -- Fetch stats for one campaign ---------------------------------------------
def fetch_report(campaign, index, total):
    cid   = campaign.get("id", "")
    token = campaign.get("token", "")
    label = f"{index + 1}/{total} {cid}"

    if not cid or not token:
        return []

    base        = f"{API_BASE}/api/campaigns/{cid}"
    base_params = {"access_token": token, "timespan": f"0-{DAYS_BACK}day", "asTree": "true"}

    # Strategy 1: editsDef_v2 grouped by user
    data1 = fetch_json_report(
        f"{base}/reports/editsDef_v2/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "completed", "column1": "success", "column2": "successRate",
         "column3": "workTime", "column4": "success_p_h", "column5": "completed_p_h"},
        label, "editsDef_v2/report[user]"
    )
    if data1 is None:
        print(f"  [{label}] 403 on first call - campaign token invalid, skipping")
        return []
    if isinstance(data1, dict) and data1:
        rows = extract_rows_from_tree(data1, label)
        if rows:
            return rows

    # Strategy 2: dialerStat grouped by user (has norespons/answeringmachines)
    data2 = fetch_json_report(
        f"{base}/reports/dialerStat/report/{LOCALE}",
        {**base_params, "group0": "user",
         "column0": "count", "column1": "connects",
         "column2": "answeringmachines", "column3": "norespons", "column4": "connectRate"},
        label, "dialerStat/report[user]"
    )
    if data2 is None:
        return []
    if isinstance(data2, dict) and data2:
        rows = extract_rows_from_tree(data2, label)
        if rows:
            return rows

    # Strategy 3: editsDef_v2 grouped by date then user (nested)
    data3 = fetch_json_report(
        f"{base}/reports/editsDef_v2/report/{LOCALE}",
        {**base_params, "group0": "date", "group1": "user",
         "column0": "completed", "column1": "success",
         "column2": "successRate", "column3": "workTime"},
        label, "editsDef_v2/report[date,user]"
    )
    if data3 is None:
        return []
    if isinstance(data3, dict) and data3:
        rows = extract_rows_from_tree(data3, label)
        if rows:
            return rows

    print(f"  [{label}] No agent rows extracted from any strategy")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    now_sast = now_utc.astimezone(TIMEZONE)

    period_end   = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end}  ({DAYS_BACK} days, asTree=true JSON)")

    tenant_id = os.environ.get("DIALFIRE_TENANT_ID", "")
    print(f"Tenant : {'***' if tenant_id else '(not set)'}")

    # Collect campaigns from CAMPAIGN_n_ID/TOKEN secrets
    campaigns = []
    i = 1
    while True:
        cid  = os.environ.get(f"CAMPAIGN_{i}_ID", "").strip()
        ctok = os.environ.get(f"CAMPAIGN_{i}_TOKEN", "").strip()
        if not cid:
            break
        if ctok:
            campaigns.append({"id": cid, "token": ctok})
            print(f"  Campaign {i}: {cid} (token=***)")
        else:
            print(f"  Campaign {i}: {cid} (NO TOKEN - skipping)")
        i += 1

    if not campaigns:
        print("No campaigns configured. Set CAMPAIGN_1_ID + CAMPAIGN_1_TOKEN secrets.")
        return

    print(f"Using {len(campaigns)} campaign(s) from CAMPAIGN_n_ID/TOKEN secrets")
    print(f"Active campaigns: {len(campaigns)}")
    print()

    # Fetch per-campaign rows
    all_rows = []
    for idx, campaign in enumerate(campaigns):
        rows = fetch_report(campaign, idx, len(campaigns))
        all_rows.extend(rows)

    print()
    print(f"Raw agent rows collected: {len(all_rows)}")
    rows_with_calls = sum(1 for r in all_rows if _safe_int(r.get("completed", r.get("count", r.get("calls", 0)))) > 0)
    print(f"Raw agent rows with calls > 0: {rows_with_calls}")

    # Merge rows by agent name
    merged = {}
    for row in all_rows:
        agent = parse_row(row)
        if agent is None:
            continue
        name = agent["name"]
        if name in merged:
            ex = merged[name]
            ex["calls"]     += agent["calls"]
            ex["success"]   += agent["success"]
            ex["declines"]  += agent["declines"]
            ex["workHours"]  = round(ex["workHours"] + agent["workHours"], 2)
            ex["cph"]        = round(ex["calls"] / ex["workHours"], 1) if ex["workHours"] > 0 else 0.0
            ex["successRate"]= round(ex["success"] / ex["calls"] * 100, 1) if ex["calls"] > 0 else 0.0
            ex["meetsTarget"]= ex["cph"] >= BENCHMARKS["cph"]
        else:
            merged[name] = agent

    print(f"Unique agents after merge: {len(merged)}")

    rm_agents    = []
    fancy_agents = []
    for name, agent in sorted(merged.items()):
        if name in RM_NAMES:
            rm_agents.append(agent)
        else:
            fancy_agents.append(agent)

    rm_agents.sort(key=lambda x: x["calls"], reverse=True)
    fancy_agents.sort(key=lambda x: x["calls"], reverse=True)

    print(f"RM: {len(rm_agents)} | Fancy Callers: {len(fancy_agents)}")
    if rm_agents:
        print("RM agents:")
        for a in rm_agents:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")
    if fancy_agents:
        print("Fancy Callers (top 15):")
        for a in fancy_agents[:15]:
            print(f"  {a['name']:25s} calls={a['calls']:4d}  success={a['success']:4d}  declines={a['declines']:4d}  cph={a['cph']:5.1f}  sr={a['successRate']:5.1f}%")

    # Build output
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    weekly_path = os.path.join(data_dir, "weekly_data.json")
    weekly = {
        "generated":   now_utc.isoformat(),
        "periodStart": str(period_start),
        "periodEnd":   str(period_end),
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }
    with open(weekly_path, "w") as f:
        json.dump(weekly, f, indent=2)
    print(f"Saved weekly_data.json  (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    # Update history
    history_path = os.path.join(data_dir, "history.json")
    try:
        with open(history_path) as f:
            raw_hist = json.load(f)
    except Exception:
        raw_hist = []
    history = [h for h in raw_hist if isinstance(h, dict) and "weekStart" in h]

    week_key = str(period_start)
    history  = [h for h in history if h.get("weekStart") != week_key]
    history.append({
        "weekStart": week_key,
        "weekEnd":   str(period_end),
        "generated": now_utc.isoformat(),
        "rm":        rm_agents,
        "fancy":     fancy_agents,
    })
    history.sort(key=lambda x: x["weekStart"])
    history = history[-12:]

    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json  ({len(history)} weeks)")


if __name__ == "__main__":
    main()
