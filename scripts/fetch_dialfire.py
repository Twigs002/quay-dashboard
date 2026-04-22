"""
DialFire Campaign Stats -> weekly_data.json fetcher
====================================================
Fetches agent stats from DialFire API using per-campaign tokens.

API response format (asTree):
  groups is a list of {"value": "AgentName", "columns": [v0, v1, ...]}
  where column order matches columnDefs order.

Leads and email counts:
  Fetched via a second report call (editsDef_v2 grouped by user+disposition).
  Disposition-to-field mapping:
    seller: dispositions containing 'lead' or 'SELLER_LEAD'
    rental: dispositions containing 'rental' or 'RENTAL_LEAD'
    email:  any disposition where hs_lead_status != 'NOT_ENGAGING'
            (i.e. email was obtained)
"""

import os, json, re, time, datetime, pytz
import requests

# -- Config -------------------------------------------------------------------
LOCALE = "en_US"
DAYS_BACK = 7
TIMEZONE = pytz.timezone("Africa/Johannesburg")
API_BASE = "https://api.dialfire.com"

BENCHMARKS = {
    "cph": 45,
    "daily_calls": 315,
    "rm_success_rate": 17,
    "fc_success_rate": 20,
}

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

# Disposition keywords (lowercase) -> lead type
# Adjust these based on your actual Dialfire disposition names
SELLER_KEYS = {"seller", "seller_lead", "selling", "for_sale"}
RENTAL_KEYS = {"rental", "rental_lead", "renting", "for_rent", "tenant"}
# Email is obtained when hs_lead_status is NOT one of these values:
NO_EMAIL_STATUSES = {"not_engaging", "no_email", "no_contact", "bad_number", "do_not_contact"}


# -- Poll helper --------------------------------------------------------------
def fetch_json(url, params, label, tag, timeout=30, max_polls=8):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 202:
            poll_url = r.json().get("url") or r.json().get("statusUrl")
            if not poll_url:
                return []
            for _ in range(max_polls):
                time.sleep(2)
                r2 = requests.get(poll_url, timeout=timeout)
                if r2.status_code == 200:
                    print(f"  [{label}] {tag} -> HTTP 200 (after poll)")
                    try:
                        return r2.json()
                    except Exception:
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
        if r.status_code == 403:
            return None
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

def _col_names(col_defs):
    if not col_defs:
        return []
    names = []
    for cd in col_defs:
        if isinstance(cd, str):
            names.append(cd)
        elif isinstance(cd, dict):
            names.append(cd.get("name") or cd.get("id") or cd.get("key") or "")
        else:
            names.append("")
    return names


# -- Row extractor ------------------------------------------------------------
def extract_rows(data, label):
    if not isinstance(data, dict):
        return []

    col_defs   = data.get("columnDefs", [])
    grp_names  = _col_names(col_defs)
    groups_raw = data.get("groups", [])

    print(f"  [{label}] DIAG grpDefs={grp_names} groups={type(groups_raw).__name__}[{len(groups_raw) if hasattr(groups_raw,'__len__') else '?'}]")

    def _cn(cd):
        return _col_names(cd) or grp_names

    def _parse_group_item(item, cn):
        if not isinstance(item, dict):
            return None
        if "value" in item and "columns" in item:
            cols = item["columns"]
            name = str(item["value"])
            d = {"name": name}
            if isinstance(cols, list):
                for i, v in enumerate(cols):
                    key = cn[i] if i < len(cn) else f"col{i}"
                    d[key] = v
            elif isinstance(cols, dict):
                d.update(cols)
            return d
        if any(k in item for k in ("user", "name", "username", "agent")):
            name = (item.get("user") or item.get("name") or
                    item.get("username") or item.get("agent") or "")
            d = {"name": str(name)}
            for k, v in item.items():
                if k not in ("user", "name", "username", "agent"):
                    d[k] = v
            return d
        return None

    rows = []
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    if isinstance(groups_raw, list):
        sample_values = []
        for item in groups_raw[:3]:
            if isinstance(item, dict):
                v = str(item.get("value", item.get("name", item.get("user", ""))))
                if v:
                    sample_values.append(v)

        is_nested = (bool(sample_values) and
                     all(date_re.match(v) for v in sample_values) and
                     len(grp_names) > 1)

        if is_nested:
            agg = {}
            for date_item in groups_raw:
                if not isinstance(date_item, dict):
                    continue
                date_val = str(date_item.get("value", date_item.get("name", "")))
                if not date_re.match(date_val):
                    continue
                inner_raw = date_item.get("groups", date_item.get("children", []))
                inner_cols = _cn(date_item.get("columnDefs", col_defs))
                for inner in (inner_raw or []):
                    parsed = _parse_group_item(inner, inner_cols)
                    if parsed:
                        n = parsed.get("name", "")
                        if n not in agg:
                            agg[n] = parsed.copy()
                        else:
                            for k, v in parsed.items():
                                if k != "name":
                                    try:
                                        agg[n][k] = float(agg[n].get(k, 0)) + float(v or 0)
                                    except (TypeError, ValueError):
                                        pass
            rows = list(agg.values())
        else:
            for item in groups_raw:
                parsed = _parse_group_item(item, _cn(col_defs))
                if parsed:
                    rows.append(parsed)

    elif isinstance(groups_raw, dict):
        for agent_name, cols in groups_raw.items():
            if isinstance(cols, dict):
                d = {"name": agent_name}
                d.update(cols)
                rows.append(d)
            else:
                rows.append({"name": agent_name})

    print(f"  [{label}] extracted {len(rows)} rows")
    if rows:
        print(f"  [{label}] sample row: {rows[0]}")
    return rows


# -- Fetch disposition counts per agent --------------------------------------
def fetch_dispositions(cid, token, ts, label):
    """
    Fetch a disposition-grouped report to count seller/rental/email per agent.
    Returns dict: {agent_name: {"seller": N, "rental": N, "email": N}}
    """
    base = f"{API_BASE}/api/campaigns/{cid}"
    result = {}

    # Try nested grouping: group0=user, group1=disposition, column0=completed
    params = {
        "access_token": token,
        "asTree": "true",
        "timespan": ts,
        "group0": "user",
        "group1": "disposition",
        "column0": "completed",
    }
    data = fetch_json(f"{base}/reports/editsDef_v2/report/{LOCALE}", params,
                      label, "disposition report")

    if not isinstance(data, dict):
        return result

    groups = data.get("groups", [])
    if not isinstance(groups, list):
        return result

    print(f"  [{label}] disposition groups: {len(groups)}")

    for agent_item in groups:
        if not isinstance(agent_item, dict):
            continue
        agent_name = str(agent_item.get("value", agent_item.get("name", agent_item.get("user", "")))).strip()
        if not agent_name:
            continue

        # Inner groups are dispositions
        inner_groups = agent_item.get("groups", agent_item.get("children", []))
        if not isinstance(inner_groups, list):
            continue

        seller = 0
        rental = 0
        email  = 0

        for disp_item in inner_groups:
            if not isinstance(disp_item, dict):
                continue
            disp_name = str(disp_item.get("value", disp_item.get("name", ""))).lower().strip()
            cols = disp_item.get("columns", [])
            count = _safe_int(cols[0] if isinstance(cols, list) and cols else disp_item.get("completed", 0))

            if any(k in disp_name for k in SELLER_KEYS):
                seller += count
            if any(k in disp_name for k in RENTAL_KEYS):
                rental += count
            # Email: any disposition that is NOT a no-email status
            if count > 0 and not any(k in disp_name for k in NO_EMAIL_STATUSES):
                email += count

        if agent_name not in result:
            result[agent_name] = {"seller": 0, "rental": 0, "email": 0}
        result[agent_name]["seller"] += seller
        result[agent_name]["rental"] += rental
        result[agent_name]["email"]  += email

    print(f"  [{label}] disposition result: {result}")
    return result


# -- Parse one row into agent dict -------------------------------------------
def parse_row(row):
    name = str(row.get("name", row.get("user", row.get("agent", "")))).strip()
    if not name or name in ("", "null", "None"):
        return None

    calls   = _safe_int(row.get("completed", row.get("calls", 0)))
    success = _safe_int(row.get("success", 0))
    seller  = _safe_int(row.get("seller", 0))
    rental  = _safe_int(row.get("rental", 0))
    email   = _safe_int(row.get("email", row.get("gotEmail", row.get("got_email", 0))))

    wt_raw    = _safe_float(row.get("workTime", row.get("workHours", 0)))
    work_time = round(wt_raw / 3600, 2) if wt_raw > 100 else round(wt_raw, 2)

    sr = row.get("successRate", "")
    if sr == "" or sr is None:
        sr = round(success / calls * 100, 1) if calls > 0 else 0.0
    else:
        sr = _safe_float(sr)
        if 0.0 < sr <= 1.0 and success > 0 and calls > 0:
            computed = success / calls
            if abs(sr - computed) < 0.01:
                sr = round(sr * 100, 1)

    cph = round(calls / work_time, 1) if work_time > 0 else 0.0

    return {
        "name": name,
        "calls": calls,
        "success": success,
        "seller": seller,
        "rental": rental,
        "email": email,
        "cph": cph,
        "successRate": round(sr, 1),
        "workTime": work_time,
        "meetsTarget": cph >= BENCHMARKS["cph"],
    }


# -- Fetch one campaign -------------------------------------------------------
def fetch_campaign(cid, token, index, total):
    label = f"{index + 1}/{total} {cid}"
    base = f"{API_BASE}/api/campaigns/{cid}"

    timespans = ["0-0day", f"{DAYS_BACK}-0day", "14-0day", "7-0day", "30-0day"]

    for ts in timespans:
        params = {
            "access_token": token,
            "asTree": "true",
            "timespan": ts,
            "group0": "user",
            "column0": "completed",
            "column1": "success",
            "column2": "successRate",
            "column3": "workTime",
        }
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
                    # Also fetch disposition counts
                    disps = fetch_dispositions(cid, token, ts, label)
                    # Merge disposition counts into rows
                    for row in rows:
                        name = row.get("name", "")
                        if name in disps:
                            row["seller"] = disps[name]["seller"]
                            row["rental"] = disps[name]["rental"]
                            row["email"]  = disps[name]["email"]
                    return rows
        else:
            print(f"  [{label}] ts={ts} got non-dict: {type(data).__name__}")

    print(f"  [{label}] all timespans failed")
    return []


# -- Main ---------------------------------------------------------------------
def main():
    now_utc    = datetime.datetime.now(datetime.timezone.utc)
    now_sast   = now_utc.astimezone(TIMEZONE)
    period_end   = now_sast.date()
    period_start = period_end - datetime.timedelta(days=DAYS_BACK)

    print("=== DialFire Weekly Fetch ===")
    print(f"Period : {period_start} to {period_end} ({DAYS_BACK} days)")

    campaigns = []

    ch_id  = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
    ch_tok = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
    if ch_id and ch_tok:
        campaigns.append({"id": ch_id, "token": ch_tok, "label": "CLIENTHUB"})
        print(f"  CLIENTHUB campaign: {ch_id}")
    elif ch_id:
        print(f"  CLIENTHUB campaign: {ch_id} (NO TOKEN)")

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
            print(f"  Campaign {i}: {cid} (NO TOKEN)")
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
            ex["calls"]    += agent["calls"]
            ex["success"]  += agent["success"]
            ex["seller"]   += agent["seller"]
            ex["rental"]   += agent["rental"]
            ex["email"]    += agent["email"]
            ex["workTime"]  = round(ex["workTime"] + agent["workTime"], 2)
            ex["cph"]       = round(ex["calls"] / ex["workTime"], 1) if ex["workTime"] > 0 else 0.0
            ex["successRate"] = round(ex["success"] / ex["calls"] * 100, 1) if ex["calls"] > 0 else 0.0
            ex["meetsTarget"] = ex["cph"] >= BENCHMARKS["cph"]
        else:
            merged[name] = agent

    agents       = list(merged.values())
    rm_agents    = sorted([a for a in agents if a["name"] in RM_NAMES],    key=lambda x: -x["calls"])
    fancy_agents = sorted([a for a in agents if a["name"] not in RM_NAMES], key=lambda x: -x["calls"])

    print(f"Unique agents: {len(agents)}")
    print(f"RM: {len(rm_agents)} | Fancy: {len(fancy_agents)}")
    for a in rm_agents:
        print(f"  RM   {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5}")
    for a in fancy_agents:
        print(f"  FANCY {a['name']:<22} calls={a['calls']:>4} success={a['success']:>3} "
              f"seller={a['seller']:>3} rental={a['rental']:>3} email={a['email']:>3} cph={a['cph']:>5}")

    week_str = str(period_start)
    output = {
        "generated":   now_utc.isoformat(),
        "week":        week_str,
        "periodStart": str(period_start),
        "periodEnd":   str(period_end),
        "rm":          rm_agents,
        "fancy":       fancy_agents,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/weekly_data.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"Saved weekly_data.json (rm={len(rm_agents)}, fancy={len(fancy_agents)})")

    hist_path = "data/history.json"
    try:
        with open(hist_path) as f:
            history = json.load(f)
        if not isinstance(history, list):
            history = []
    except (FileNotFoundError, json.JSONDecodeError):
        history = []

    week_entry = {
        "weekStart": str(period_start),
        "weekEnd":   str(period_end),
        "week":      week_str,
        "generated": now_utc.isoformat(),
        "rm":        rm_agents,
        "fancy":     fancy_agents,
    }
    replaced = False
    for idx2, h in enumerate(history):
        if h.get("weekStart") == str(period_start) or h.get("week") == week_str:
            history[idx2] = week_entry
            replaced = True
            break
    if not replaced:
        history.append(week_entry)

    history = history[-52:]
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Updated history.json ({len(history)} weeks)")


if __name__ == "__main__":
    main()
