"""
DialFire Multi-Campaign fetcher
Auth: Authorization: Bearer {token} header (per official DialFire API docs)
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone

raw_campaigns = os.environ.get("DIALFIRE_CAMPAIGNS", "[]")
try:
    CAMPAIGNS = json.loads(raw_campaigns)
except json.JSONDecodeError as e:
    print(f"ERROR Could not parse DIALFIRE_CAMPAIGNS secret: {e}")
    raise

if not CAMPAIGNS:
    raise ValueError("DIALFIRE_CAMPAIGNS secret is empty.")

print(f"Loaded {len(CAMPAIGNS)} campaigns from secret")

today    = datetime.now(timezone.utc).date()
last_mon = today - timedelta(days=today.weekday() + 7)
last_sun = last_mon + timedelta(days=6)
DATE_FROM = last_mon.strftime("%Y-%m-%d")
DATE_TO   = last_sun.strftime("%Y-%m-%d")

RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}

def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)

def fetch_campaign(campaign):
    cid   = campaign["id"]
    token = campaign["token"]
    label = campaign.get("name", cid)

    # Per official DialFire docs: Bearer token in Authorization header
    headers = {"Authorization": f"Bearer {token}"}
    base    = f"https://api.dialfire.com/api/campaigns/{cid}"

    templates = ["editsDef_v2", "dialerStat", "activities", "addressesStat_v2"]

    for template in templates:
        url = f"{base}/reports/{template}/report/de_DE"

        # First try with columns
        params = {
            "asTree": "true",
            "group0": "date",
            "group1": "user",
            "from":   DATE_FROM,
            "to":     DATE_TO,
        }
        if template == "editsDef_v2":
            params.update({"column0": "completed", "column1": "success", "column2": "workTime"})
        elif template == "dialerStat":
            params.update({"column0": "count", "column1": "connects", "column2": "workTime"})

        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            print(f"  [{label}] {template} -> HTTP {r.status_code}")

            if r.status_code == 401:
                print(f"  [{label}] 401 Unauthorized — token rejected")
                return []
            if r.status_code in (403, 404):
                continue
            if r.status_code == 500:
                # Retry without columns
                params2 = {"asTree": "true", "group0": "date", "group1": "user",
                           "from": DATE_FROM, "to": DATE_TO}
                r = requests.get(url, headers=headers, params=params2, timeout=30)
                print(f"  [{label}] {template} no-columns -> HTTP {r.status_code}")
                if r.status_code != 200:
                    print(f"  [{label}] {r.text[:150]}")
                    continue

            if r.status_code != 200:
                print(f"  [{label}] HTTP {r.status_code}: {r.text[:150]}")
                continue

            raw  = r.json()
            rows = extract_rows(raw, label)
            if rows:
                print(f"  OK [{label}] {len(rows)} agent rows via {template}")
                return rows
            else:
                print(f"  [{label}] {template} — 0 rows, trying next template")
                continue

        except Exception as e:
            print(f"  [{label}] Error: {e}")
            continue

    print(f"  FAIL [{label}] No data from any template")
    return []

def extract_rows(raw, label):
    if isinstance(raw, list):
        return flatten_groups(raw)
    if isinstance(raw, dict):
        if "groups" in raw:
            g = raw["groups"]
            if isinstance(g, list) and len(g) > 0:
                print(f"    [{label}] {len(g)} date groups, first keys: {list(g[0].keys()) if isinstance(g[0], dict) else '?'}")
                # Print sample of first group so we can see structure
                print(f"    [{label}] First group sample: {str(g[0])[:200]}")
            elif isinstance(g, list) and len(g) == 0:
                print(f"    [{label}] groups is empty — no data for this period")
                return []
            return flatten_groups(g)
        for key in ("data", "rows", "items", "result"):
            if key in raw and isinstance(raw[key], list):
                return raw[key]
        print(f"    [{label}] Response keys: {list(raw.keys())}")
        print(f"    [{label}] Response sample: {str(raw)[:300]}")
    return []

def flatten_groups(groups, depth=0):
    rows = []
    if depth > 5 or not isinstance(groups, list):
        return rows
    for node in groups:
        if not isinstance(node, dict):
            continue
        key        = node.get("key", "")
        values     = node.get("values") or {}
        sub_groups = node.get("groups")
        if sub_groups:
            rows.extend(flatten_groups(sub_groups, depth + 1))
        else:
            row = {"name": key}
            if isinstance(values, dict):
                row.update(values)
            rows.append(row)
    return rows

def parse_row(row, campaign_name):
    name = (
        row.get("name") or row.get("key") or row.get("user") or
        row.get("agent_name") or row.get("username") or "Unknown"
    )
    if isinstance(name, dict):
        name = name.get("label") or name.get("value") or "Unknown"
    name = str(name).strip()

    def safe_int(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try: return int(float(str(v)))
                except: pass
        return 0

    def safe_float(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and v != "":
                try: return float(str(v))
                except: pass
        return 0.0

    calls     = safe_int("completed", "calls", "count", "connects")
    success   = safe_int("success", "total_success")
    rental    = safe_int("RENTAL_LEAD", "rental_lead", "rental")
    seller    = safe_int("LEAD", "seller_lead", "seller")
    email     = safe_int("GOT_EMAIL", "got_email", "email")
    wt_raw    = safe_float("workTime", "work_time", "worktime", "dial_time")
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)

    return {
        "name": name, "calls": calls, "success": success,
        "rental": rental, "seller": seller, "email": email,
        "workTime": work_time, "_campaigns": [campaign_name],
    }

def merge_agents(all_rows):
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            for k in ("calls", "success", "rental", "seller", "email"):
                m[k] += row[k]
            m["workTime"]   = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())

def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))

def main():
    print(f"\n{'='*55}")
    print(f"DialFire Multi-Campaign Fetcher")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Week: {DATE_FROM} to {DATE_TO}")
    print(f"Campaigns: {len(CAMPAIGNS)}")
    print(f"{'='*55}\n")

    all_rows = []
    for i, campaign in enumerate(CAMPAIGNS, 1):
        label = campaign.get("name", campaign["id"])
        print(f"[{i}/{len(CAMPAIGNS)}] {label}")
        rows = fetch_campaign(campaign)
        for row in rows:
            parsed = parse_row(row, label)
            if parsed["calls"] > 0:
                all_rows.append(parsed)
        time.sleep(0.2)

    print(f"\nRaw rows: {len(all_rows)}")
    agents = merge_agents(all_rows)
    print(f"Unique agents: {len(agents)}")

    rm, fancy = [], []
    for a in agents:
        div   = div_string(a["_campaigns"])
        clean = {k: v for k, v in a.items() if k != "_campaigns"}
        if is_rm(a["name"]):
            rm.append(clean)
        else:
            fancy.append({**clean, "div": div})

    print(f"RM: {len(rm)}  |  Fancy Callers: {len(fancy)}")

    output = {
        "week":      DATE_FROM,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rm":        sorted(rm,    key=lambda x: x["calls"], reverse=True),
        "fancy":     sorted(fancy, key=lambda x: x["calls"], reverse=True),
    }

    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    with open(os.path.join(data_dir, "weekly_data.json"), "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved -> data/weekly_data.json")

    hist_path = os.path.join(data_dir, "history.json")
    history = {}
    if os.path.exists(hist_path):
        with open(hist_path) as f:
            history = json.load(f)
    history[DATE_FROM] = {
        "generated": output["generated"],
        "rm":        output["rm"],
        "fancy":     output["fancy"],
    }
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=2)
    print(f"Appended -> data/history.json ({len(history)} weeks stored)")

if __name__ == "__main__":
    main()
