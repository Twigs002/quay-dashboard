"""
One-shot probe: ask DialFire's editsDef_v2 endpoint for every plausible
time-related column key and print which ones return real numeric data.

Used to discover the exact column-name strings the API accepts when the
public documentation doesn't list them. Run via the
'.github/workflows/probe-columns.yml' workflow once — read its logs, then
hardcode the discovered keys into fetch_dialfire.py.
"""
import os, sys
from dialfire_common import LOCALE, API_BASE, fetch_json

CAMPAIGN_ID    = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
CAMPAIGN_TOKEN = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()

CANDIDATES = [
    "workTime",
    "talkTime", "talkTimeDialer", "talkTime_dialer", "callTime",
    "wrapTime", "wrapUpTime", "wrapuptime", "afterCallTime", "acwTime",
    "waitingTime", "waitTime", "idleTime", "readyTime",
    "handlingTime", "preparationTime", "prepTime",
    "holdTime", "pauseTime", "breakTime",
]

def probe(col):
    params = {
        "access_token": CAMPAIGN_TOKEN,
        "asTree":       "true",
        "timespan":     "7-1day",
        "group0":       "user",
        "column0":      col,
    }
    url  = f"{API_BASE}/api/campaigns/{CAMPAIGN_ID}/reports/editsDef_v2/report/{LOCALE}"
    data = fetch_json(url, params, "probe", f"col={col}")
    if data is None:
        return "AUTH_ERR", []
    if not data or "groups" not in data:
        return "NO_DATA", []
    samples = []
    nonzero = 0
    for g in data.get("groups", []):
        if not isinstance(g, dict):
            continue
        name = str(g.get("value", "")).strip()
        cols = g.get("columns", [])
        if not cols:
            continue
        val = cols[0]
        try:
            num = float(val) if val not in (None, "", "-") else 0
        except Exception:
            num = 0
        if num != 0:
            nonzero += 1
            if len(samples) < 3:
                samples.append((name, val))
    return ("FOUND" if nonzero else "ALL_ZERO"), samples

def main():
    if not (CAMPAIGN_ID and CAMPAIGN_TOKEN):
        print("ERROR: CAMPAIGN_CLIENTHUB_ID/TOKEN not set")
        sys.exit(1)
    print(f"Probing {len(CANDIDATES)} candidate column names against ClientHub Master\n")
    results = []
    for col in CANDIDATES:
        verdict, samples = probe(col)
        marker = "✓" if verdict == "FOUND" else ("·" if verdict == "ALL_ZERO" else "✗")
        sample_str = ", ".join(f"{n}={v}" for n, v in samples) if samples else ""
        line = f"  {marker} {col:<22} {verdict:<10} {sample_str}"
        print(line)
        results.append((col, verdict, samples))
    print()
    found = [c for c, v, _ in results if v == "FOUND"]
    print(f"COLUMNS_THAT_RETURN_DATA: {found}")

if __name__ == "__main__":
    main()
