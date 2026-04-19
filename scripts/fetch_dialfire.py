"""
DialFire Multi-Campaign -> weekly_data.json fetcher
=====================================================
Uses tenant API to discover all campaigns, then fetches
per-campaign dialer statistics via the dialerStat report.

KEY FINDING from logs:
  - /report/ endpoints return CSV (text/csv), not JSON
  - /metadata/ endpoints return JSON but track "edits" not calls
  - dialerStat/report returns HTTP 200 with CSV containing actual call data
  - We must parse CSV to get real call counts

Secrets required:
  DIALFIRE_TENANT_ID    - e.g. 3f88c548
  DIALFIRE_TENANT_TOKEN - tenant-level Bearer token
"""

import os, json, time, requests, csv, io
from datetime import datetime, timedelta, timezone


TENANT_ID    = os.environ.get("DIALFIRE_TENANT_ID", "").strip()
TENANT_TOKEN = os.environ.get("DIALFIRE_TENANT_TOKEN", "").strip()


if not TENANT_ID or not TENANT_TOKEN:
    raise ValueError(
        "DIALFIRE_TENANT_ID and DIALFIRE_TENANT_TOKEN secrets must be set."
    )


# ── Date range: last full Mon-Sun week ───────────────────────────
today    = datetime.now(timezone.utc).date()
last_mon = today - timedelta(days=today.weekday() + 7)
last_sun = last_mon + timedelta(days=6)
DATE_FROM = last_mon.strftime("%Y-%m-%d")
DATE_TO   = last_sun.strftime("%Y-%m-%d")
# days_back = how many days ago Monday was (from today)
DAYS_BACK = (today - last_mon).days + 1   # e.g. Mon=7 days ago + Sun=1 => 8 days span


# ── Classify RM vs Fancy Caller ──────────────────────────────────
RM_NAMES = {
    "Gio", "NaomiCiza", "Kay-LeeOrphan", "BrandonNtini",
    "SadiqaCarelse", "DeclanT", "CameronPaulse",
}


def is_rm(name):
    n = name.lower()
    return any(rm.lower() in n or n in rm.lower() for rm in RM_NAMES)


# ── Step 1: Discover all campaigns via tenant API ────────────────
def get_all_campaigns():
    url = f"https://api.dialfire.com/api/tenants/{TENANT_ID}/campaigns/"
    print(f"Fetching campaign list from tenant API...")
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {TENANT_TOKEN}"},
            timeout=30
        )
        print(f"  Tenant API -> HTTP {r.status_code}")
        if r.status_code != 200:
            print(f"  ERROR: {r.text[:300]}")
            return []
        data = r.json()
        campaigns = data if isinstance(data, list) else data.get("campaigns", [])
        print(f"  Found {len(campaigns)} campaigns total")
        return campaigns
    except Exception as e:
        print(f"  FAIL tenant API: {e}")
        return []


# ── Step 2: Fetch dialerStat report for one campaign (CSV) ──────
def fetch_report(campaign):
    cid   = campaign.get("id", "")
    label = campaign.get("title") or campaign.get("name") or cid
    token = campaign.get("permissions", {}).get("token", "")

    if not token:
        return []

    base = f"https://api.dialfire.com/api/campaigns/{cid}"

    # dialerStat/report returns CSV with actual call counts per user
    # Use timespan parameter: "0-Nday" means last N days
    # We try with group0=user to get per-agent breakdown
    # Locale must include timezone per DialFire docs: de_DE/Africa/Johannesburg
    locale = "de_DE/Africa/Johannesburg"

    attempts = [
        # (url_suffix, params_extra, description)
        (
            f"{base}/reports/dialerStat/report/{locale}",
            {"group0": "user", "timespan": f"0-{DAYS_BACK}day"},
            "dialerStat/report[group=user]"
        ),
        (
            f"{base}/reports/dialerStat/report/{locale}",
            {"timespan": f"0-{DAYS_BACK}day"},
            "dialerStat/report[minimal]"
        ),
        # Fallback: metadata endpoint returns JSON (but tracks edits not calls)
        (
            f"{base}/reports/dialerStat/metadata/{locale.split('/')[0]}",
            {"days": str(DAYS_BACK), "group0": "user"},
            "dialerStat/metadata[group=user]"
        ),
    ]

    for url, extra_params, tag in attempts:
        params = {"_token_": token}
        params.update(extra_params)

        rows = _try_fetch(url, params, label, tag)
        if rows is None:
            return []  # 401 bad token - stop
        if rows:
            return rows

    print(f"  FAIL [{label}] No data from any combination")
    return []


# ── HTTP fetch + parse (CSV or JSON) ────────────────────────────
def _try_fetch(url, params, label, tag):
    """
    Make a request and return rows if successful, else [].
    Handles HTTP 202 (async) by polling.
    Handles both CSV and JSON responses.
    Returns None on 401 (bad token).
    """
    try:
        r = requests.get(url, params=params, timeout=30)
        status_line = f"  [{label}] {tag} -> HTTP {r.status_code}"

        # ── 202: async report, poll ───────────────────────────
        if r.status_code == 202:
            print(f"{status_line}  (async, polling...)")
            for attempt in range(5):
                time.sleep(5)
                r = requests.get(url, params=params, timeout=30)
                if r.status_code == 200:
                    break
                if r.status_code == 202:
                    print(f"    [{label}] still 202, attempt {attempt+1}/5...")
                    continue
                break
            status_line = f"  [{label}] {tag} -> HTTP {r.status_code} (after poll)"

        if r.status_code == 401:
            print(f"{status_line}  (bad token)")
            return None
        if r.status_code in (403, 404):
            print(f"{status_line}")
            return []
        if r.status_code != 200:
            snippet = r.text[:120].replace("\n", " ")
            print(f"{status_line}  {snippet}")
            return []

        ct = r.headers.get("Content-Type", "")

        # ── CSV response ──────────────────────────────────────
        if "text/csv" in ct or "text/plain" in ct:
            rows = parse_csv_response(r.text, label, tag)
            if rows:
                print(f"{status_line}  -> {len(rows)} CSV rows  ✓")
            else:
                print(f"{status_line}  -> 0 CSV rows (empty or no user column)")
            return rows

        # ── JSON response ─────────────────────────────────────
        try:
            raw = r.json()
        except Exception:
            print(f"{status_line}  (not JSON, content-type={ct}, preview: {r.text[:80]!r})")
            return []

        rows = extract_rows(raw, label, tag)
        if rows:
            print(f"{status_line}  -> {len(rows)} rows  ✓")
        else:
            print(f"{status_line}  -> 0 rows")
        return rows

    except requests.RequestException as e:
        print(f"  [{label}] {tag} -> network error: {e}")
        return []


# ── Parse CSV response into list of dicts ────────────────────────
def parse_csv_response(text, label, tag):
    """
    DialFire CSV reports have a header row then data rows.
    Typical dialerStat columns (may vary by locale/config):
      User, Completed, Success, Work time, Waiting time, Talk time, ...
    We normalise column names to lowercase with underscores.
    """
    if not text or not text.strip():
        return []

    try:
        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        rows = []
        for row in reader:
            # Normalise keys: strip whitespace, lowercase
            normalised = {k.strip().lower().replace(" ", "_"): v.strip()
                          for k, v in row.items() if k}
            if normalised:
                rows.append(normalised)

        if not rows:
            # Try comma delimiter
            reader2 = csv.DictReader(io.StringIO(text), delimiter=",")
            for row in reader2:
                normalised = {k.strip().lower().replace(" ", "_"): v.strip()
                              for k, v in row.items() if k}
                if normalised:
                    rows.append(normalised)

        # Print sample of column names for debugging
        if rows:
            sample_keys = list(rows[0].keys())[:10]
            print(f"    [{label}] CSV columns: {sample_keys}")

        return rows

    except Exception as e:
        print(f"    [{label}] CSV parse error: {e}")
        return []


# ── Parse JSON response shapes ────────────────────────────────────
def extract_rows(raw, label, tag=""):
    """Parse any DialFire JSON response shape into a flat list of dicts."""
    if isinstance(raw, dict):
        keys = list(raw.keys())[:8]
        grp  = raw.get("groups")
        grp_len = (len(grp) if isinstance(grp, list)
                   else "dict" if isinstance(grp, dict)
                   else type(grp).__name__ if grp is not None
                   else "missing")
        print(f"    [{label}] keys={keys}  groups={grp_len}")

    if isinstance(raw, list):
        return flatten_groups(raw)

    if not isinstance(raw, dict):
        return []

    if "groups" in raw:
        g = raw["groups"]
        if isinstance(g, list):
            if g:
                first_keys = list(g[0].keys()) if isinstance(g[0], dict) else []
                print(f"    [{label}] first group keys: {first_keys}")
            return flatten_groups(g)

        if isinstance(g, dict) and g:
            print(f"    [{label}] groups is dict with {len(g)} keys, sample: {list(g.keys())[:5]}")
            rows = []
            for key, val in g.items():
                if not isinstance(val, dict):
                    continue
                if "groups" in val:
                    sub = val["groups"]
                    if isinstance(sub, dict):
                        for subkey, subval in sub.items():
                            if isinstance(subval, dict):
                                row = {"name": subkey}
                                row.update(subval.get("values", subval))
                                rows.append(row)
                    elif isinstance(sub, list):
                        rows.extend(flatten_groups(sub))
                else:
                    values = val.get("values", val)
                    row = {"name": key}
                    if isinstance(values, dict):
                        row.update(values)
                    rows.append(row)
            return rows

    for key in ("data", "rows", "records", "items", "result"):
        if key in raw and isinstance(raw[key], list):
            return raw[key]

    if "key" in raw:
        return [raw]

    return []


def flatten_groups(groups, depth=0):
    """Recursively flatten DialFire groups tree into agent rows."""
    rows = []
    if depth > 6 or not isinstance(groups, list):
        return rows
    for node in groups:
        if not isinstance(node, dict):
            continue
        values     = node.get("values") or {}
        sub_groups = node.get("groups")
        if sub_groups and isinstance(sub_groups, list) and sub_groups:
            rows.extend(flatten_groups(sub_groups, depth + 1))
        else:
            key = node.get("key", "")
            row = {"name": key}
            if isinstance(values, dict):
                row.update(values)
            rows.append(row)
    return rows


# ── Parse one row into our standard schema ────────────────────────
def parse_row(row, campaign_name):
    """
    Handles both JSON rows (from metadata) and CSV rows (from report).
    CSV column names from dialerStat typically include:
      'user', 'completed', 'success', 'work_time', 'rental_lead',
      'seller_lead', 'got_email' (varies by campaign config)
    """
    # Get agent name - CSV uses 'user', JSON uses 'name'/'key'
    name = (
        row.get("user") or row.get("name") or row.get("key") or
        row.get("agent_name") or row.get("username") or "Unknown"
    )
    if isinstance(name, dict):
        name = name.get("label") or name.get("value") or "Unknown"
    name = str(name).strip()

    # Skip system/empty/total rows
    if not name or name.lower() in ("unknown", "system", "", "total", "--"):
        return None

    def safe_int(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip() not in ("", "-", "N/A"):
                try:
                    return int(float(str(v).replace(",", ".")))
                except (ValueError, TypeError):
                    pass
        return 0

    def safe_float(*keys):
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip() not in ("", "-", "N/A"):
                try:
                    return float(str(v).replace(",", "."))
                except (ValueError, TypeError):
                    pass
        return 0.0

    # CSV columns (dialerStat): completed, success, rental_lead, seller_lead,
    #                            got_email, work_time (in seconds or hours)
    # JSON columns (metadata):  completed, success, RENTAL_LEAD, SELLER_LEAD,
    #                            GOT_EMAIL, workTime
    calls   = safe_int("completed", "total_calls", "calls", "count", "connects")
    success = safe_int("success", "total_success")
    rental  = safe_int("rental_lead", "RENTAL_LEAD", "rental")
    seller  = safe_int("seller_lead", "SELLER_LEAD", "seller")
    email   = safe_int("got_email", "GOT_EMAIL", "email")

    wt_raw    = safe_float("work_time", "workTime", "worktime", "dial_time")
    # If work_time > 1000, it's in seconds; otherwise it's already in hours
    work_time = round(wt_raw / 3600, 2) if wt_raw > 1000 else round(wt_raw, 2)

    return {
        "name":       name,
        "calls":      calls,
        "success":    success,
        "rental":     rental,
        "seller":     seller,
        "email":      email,
        "workTime":   work_time,
        "_campaigns": [campaign_name],
    }


# ── Merge agents across campaigns ────────────────────────────────
def merge_agents(all_rows):
    merged = {}
    for row in all_rows:
        name = row["name"]
        if not name or name.lower() in ("unknown", "system", ""):
            continue
        if name in merged:
            m = merged[name]
            m["calls"]    += row["calls"]
            m["success"]  += row["success"]
            m["rental"]   += row["rental"]
            m["seller"]   += row["seller"]
            m["email"]    += row["email"]
            m["workTime"]  = round(m["workTime"] + row["workTime"], 2)
            m["_campaigns"] = list(set(m["_campaigns"] + row["_campaigns"]))
        else:
            merged[name] = dict(row)
    return list(merged.values())


def div_string(campaigns_list):
    return " / ".join(sorted(set(c for c in campaigns_list if c)))


# ── Main ─────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"DialFire Multi-Campaign Fetcher  (tenant API)")
    print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Week: {DATE_FROM} to {DATE_TO}  (days_back={DAYS_BACK})")
    print(f"Tenant ID: {TENANT_ID}")
    print(f"{'='*60}\n")

    campaigns = get_all_campaigns()
    if not campaigns:
        raise RuntimeError("No campaigns found from tenant API")

    active = [c for c in campaigns if not c.get("hidden", False)]
    print(f"\nProcessing {len(active)} active campaigns (of {len(campaigns)} total)\n")

    all_rows = []
    for i, campaign in enumerate(active, 1):
        label = campaign.get("title") or campaign.get("name") or campaign.get("id", "?")
        cid   = campaign.get("id", "")
        token = campaign.get("permissions", {}).get("token", "")
        if not token:
            print(f"[{i}/{len(active)}] SKIP {label} (no token)")
            continue

        print(f"[{i}/{len(active)}] {label}  ({cid})")
        rows = fetch_report(campaign)
        for row in rows:
            parsed = parse_row(row, label)
            if parsed and parsed["calls"] > 0:
                all_rows.append(parsed)
        time.sleep(0.3)

    print(f"\n{'─'*50}")
    print(f"Raw agent rows with calls > 0: {len(all_rows)}")

    agents = merge_agents(all_rows)
    print(f"Unique agents after merge:     {len(agents)}")

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
    os.makedirs(data_dir, exist_ok=True)

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
