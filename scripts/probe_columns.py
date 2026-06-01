"""Confirm the column keys discovered from saved-report definitions."""
import os, sys
from dialfire_common import LOCALE, API_BASE, fetch_json

CID = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
TOK = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
if not (CID and TOK):
    print("ERROR: creds missing"); sys.exit(1)

# Known good (from saved reports): talkTimeDialerShare, wrapupShare,
# connectTimeDialer, workTime.  Also probe a couple variants for the
# "work time %" question.
CANDS = [
    "talkTimeDialerShare", "wrapupShare", "connectTimeDialer",
    "workTimeShare", "workShare", "pauseTimeShare", "pauseShare",
    "waitingTimeDialerShare", "waitingTimeShare",
    "talkTimeShare", "talkShare",
    "handlingTimeShare", "preparationTimeShare",
    "connectTime", "talkTime", "wrapupTime", "wrapTime",
    "waitingTimeDialer", "handlingTime", "preparationTime",
]

for col in CANDS:
    params = {"access_token": TOK, "asTree": "true", "timespan": "7-1day",
              "group0": "user", "column0": col}
    url  = f"{API_BASE}/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}"
    data = fetch_json(url, params, "probe", f"col={col}")
    if not isinstance(data, dict):
        print(f"  ? {col}  (no data)"); continue
    samples = []
    nonzero = 0
    for g in data.get("groups", []):
        if not isinstance(g, dict): continue
        cols = g.get("columns") or []
        if not cols: continue
        v = cols[0]
        try: num = float(v) if v not in (None,"","-") else 0
        except: num = 0
        if num != 0:
            nonzero += 1
            if len(samples) < 3:
                samples.append((str(g.get("value","")).strip(), v))
    marker = "✓" if nonzero else "·"
    s = ", ".join(f"{n}={v}" for n,v in samples) if samples else ""
    print(f"  {marker} {col:<30} {s}")
