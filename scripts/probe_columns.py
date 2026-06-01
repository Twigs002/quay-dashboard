"""Hunt the API key for 'Waiting time (dialer)' — confirmed to exist in
the UI's column picker but returned zero on first-pass guesses."""
import os, sys
from dialfire_common import LOCALE, API_BASE, fetch_json

CID = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
TOK = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()
if not (CID and TOK):
    print("ERROR"); sys.exit(1)

CANDIDATES = [
    # NEW: try the actual chip labels seen in the activity log
    "inactiveTime", "inactivityTime", "inactive", "inactivity",
    "inactiveTimeDialer", "inactivityTimeDialer",
    "inactiveTimeShare", "inactivityShare",
    # close to known-working connectTimeDialer / wrapupTime patterns
    "waitTimeDialer", "waitingTimeDialer", "idleTimeDialer", "dialerWaitingTime",
    "dialerWaitTime", "dialerIdleTime",
    "connectingTimeDialer", "connectingTime",
    "ringTimeDialer", "ringTime",
    "dialingTime", "dialingTimeDialer",
    # standalone
    "waitingTime", "waitTime", "idleTime",
    # share variants
    "waitingTimeDialerShare", "waitTimeDialerShare", "idleTimeDialerShare",
    "inactiveTimeDialerShare",
    # "Contact edit" / "contactEdit" / "edit" might be the talk slot
    "contactEditTime", "editTime",
    # German fallback
    "wartezeit", "leerlaufzeit", "untaetigkeitszeit",
    # bonus
    "handlingTimeDialer", "preparationTimeDialer",
    # backstop
    "workTime",
]

found = []
for col in CANDIDATES:
    params = {"access_token": TOK, "asTree": "true", "timespan": "7-1day",
              "group0": "user", "column0": col}
    url  = f"{API_BASE}/api/campaigns/{CID}/reports/editsDef_v2/report/{LOCALE}"
    data = fetch_json(url, params, "probe", f"col={col}")
    if not isinstance(data, dict):
        print(f"  ? {col}"); continue
    samples, nonzero = [], 0
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
    if nonzero:
        print(f"  ✓ {col:<32} {samples}")
        found.append(col)
    else:
        print(f"  · {col}")

print(f"\nFOUND: {found}")
