#!/usr/bin/env python3
"""Discovery script: probe several DialFire API endpoints against one test
campaign and dump responses so we can identify which one returns the
'tasks available to call' count for that campaign.

Run via: gh workflow run discover-pool-endpoint.yml (or workflow_dispatch on
update-data.yml after pasting). Reads CAMPAIGN_1_ID / CAMPAIGN_1_TOKEN from
env. Logs every probe + a 500-char snippet of the response."""
import json
import os
import sys
import requests

API_BASE = "https://api.dialfire.com"
CID   = os.environ.get("CAMPAIGN_1_ID")
TOKEN = os.environ.get("CAMPAIGN_1_TOKEN")
if not CID or not TOKEN:
    sys.exit("set CAMPAIGN_1_ID and CAMPAIGN_1_TOKEN")

# Candidate endpoints. Sorted from most-likely to least-likely.
PROBES = [
    ("GET", f"/api/campaigns/{CID}/tasks/_count",                  {}),
    ("GET", f"/api/campaigns/{CID}/tasks/_search",                 {"status": "available", "_count": "true"}),
    ("GET", f"/api/campaigns/{CID}/tasks",                         {"status": "available", "limit": "1"}),
    ("GET", f"/api/campaigns/{CID}/recipients/_count",             {"status": "available"}),
    ("GET", f"/api/campaigns/{CID}/recipients",                    {"status": "available", "limit": "1"}),
    ("GET", f"/api/campaigns/{CID}/status",                        {}),
    ("GET", f"/api/campaigns/{CID}",                               {}),
    ("GET", f"/api/campaigns/{CID}/stats",                         {}),
    ("GET", f"/api/campaigns/{CID}/queues",                        {}),
    ("GET", f"/api/campaigns/{CID}/queue",                         {}),
    ("GET", f"/api/campaigns/{CID}/dispositions",                  {}),
    ("GET", f"/api/campaigns/{CID}/tasks/dispositions",            {}),
]

print(f"Probing campaign {CID} ...")
for method, path, extra in PROBES:
    params = {"access_token": TOKEN, **extra}
    url = API_BASE + path
    try:
        r = requests.get(url, params=params, timeout=20, allow_redirects=False)
        status = r.status_code
        body = r.text[:500].replace("\n", " ")
        print(f"\n[{status}] GET {path}  params={list(extra)}")
        print(f"   body[:500]: {body!r}")
        if status == 202:
            loc = r.headers.get("Location") or r.headers.get("location")
            print(f"   202 poll Location: {loc!r}")
    except Exception as e:
        print(f"\n[ERR] GET {path}: {type(e).__name__}: {e}")
