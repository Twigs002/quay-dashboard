"""
DialFire metadata probe v3 — find API column keys for talk/wrap/waiting time.

Two new strategies:
  1. Properly dump the campaign-root JSON (previous run discarded it).
  2. Probe candidate column names using the Pascal_Snake_Case convention
     ("Lead_Status" is documented working — see dialfire_common.py).
  3. Try fetching the saved reports the user has in DialFire UI by likely paths.
"""
import os, sys, json, requests
from dialfire_common import LOCALE, API_BASE, fetch_json

CID = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
TOK = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()


def get(path, params=None):
    url = f"{API_BASE}{path}"
    p   = {"access_token": TOK}
    if params:
        p.update(params)
    try:
        r = requests.get(url, params=p, timeout=20)
        if r.status_code == 200:
            try:    return ("OK", r.json())
            except: return ("OK_TEXT", r.text)
        return (f"HTTP_{r.status_code}", r.text[:400])
    except Exception as e:
        return ("ERROR", str(e))


def banner(s):
    print(f"\n\n========== {s} ==========")


def main():
    if not (CID and TOK):
        print("ERROR: ClientHub creds missing"); sys.exit(1)

    # 1) Campaign root – dump full JSON to look for inline metadata.
    banner("CAMPAIGN ROOT")
    status, data = get(f"/api/campaigns/{CID}")
    print(f"status={status}")
    if isinstance(data, dict):
        print("top-level keys:", list(data.keys()))
        print(json.dumps(data, indent=2)[:5000])

    # 2) Try fetching each existing saved report by likely path names.
    banner("SAVED REPORTS – TRY KNOWN NAMES")
    saved_names = [
        "Wrap up Time", "Wrap_up_Time", "WrapUpTime",
        "Talk Time Proportion Daily", "Talk_Time_Proportion_Daily",
        "Agent Productivity", "Report",
    ]
    for name in saved_names:
        for tmpl in [
            f"/api/campaigns/{CID}/reports/{{}}",
            f"/api/campaigns/{CID}/reports/{{}}/definition",
            f"/api/campaigns/{CID}/reports/saved/{{}}",
            f"/api/campaigns/{CID}/reports/named/{{}}",
        ]:
            path = tmpl.format(requests.utils.quote(name, safe=""))
            status, body = get(path)
            if status == "OK":
                print(f"  {status}  {path}")
                print(json.dumps(body, indent=2)[:1500])

    # 3) Probe Pascal_Snake_Case columns (Lead_Status convention).
    banner("Pascal_Snake_Case COLUMN PROBE")
    pascal_cands = [
        "Work_Time", "Work_time", "work_Time",
        "Talk_Time", "Talk_Time_Dialer", "Talk_time", "Talk_time_dialer",
        "Wrap_Up_Time", "Wrap_up_Time", "Wrap_up_time", "WrapUp_Time",
        "Waiting_Time", "Waiting_Time_Dialer", "Waiting_time",
        "Handling_Time", "Preparation_Time", "Hold_Time", "Pause_Time",
        "Dialer_Talk_Time", "Dialer_Waiting_Time",
    ]
    found = []
    for col in pascal_cands:
        params = {
            "access_token": TOK,
            "asTree":       "true",
            "timespan":     "7-1day",
            "group0":       "user",
            "column0":      col,
        }
        url  = f"{API_BASE}/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}"
        data = fetch_json(url, params, "probe", f"col={col}")
        if not isinstance(data, dict):
            continue
        groups = data.get("groups", [])
        if not isinstance(groups, list):
            continue
        nonzero = 0
        samples = []
        for g in groups:
            if not isinstance(g, dict): continue
            cols = g.get("columns") or []
            if not cols: continue
            try:    v = float(cols[0]) if cols[0] not in (None,"","-") else 0
            except: v = 0
            if v != 0:
                nonzero += 1
                if len(samples) < 2:
                    samples.append((str(g.get("value","")).strip(), cols[0]))
        if nonzero:
            print(f"  ✓ {col:<30}  samples={samples}")
            found.append(col)
        else:
            print(f"  · {col}")
    print(f"\nFOUND Pascal candidates: {found}")

    # 4) Also try a few uppercase/lowercase forms.
    banner("UPPER/lower COLUMN PROBE")
    other_cands = [
        "TALK_TIME","WRAP_UP_TIME","WAITING_TIME","WORK_TIME","HANDLING_TIME",
        "talk_time","wrap_up_time","waiting_time","work_time","handling_time",
        "PREPARATION_TIME","preparation_time",
    ]
    for col in other_cands:
        params = {
            "access_token": TOK, "asTree": "true",
            "timespan": "7-1day", "group0": "user", "column0": col,
        }
        url  = f"{API_BASE}/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}"
        data = fetch_json(url, params, "probe", f"col={col}")
        if not isinstance(data, dict): continue
        nonzero = 0
        for g in data.get("groups",[]):
            if not isinstance(g,dict): continue
            cols = g.get("columns") or []
            if cols:
                try:    v = float(cols[0]) if cols[0] not in (None,"","-") else 0
                except: v = 0
                if v != 0: nonzero += 1
        marker = "✓" if nonzero else "·"
        print(f"  {marker} {col}  ({nonzero} nonzero rows)")


if __name__ == "__main__":
    main()
