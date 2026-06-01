"""Dump campaign 'favorites' + '_views_' — saved-report definitions."""
import os, sys, json, requests
from dialfire_common import API_BASE

CID = os.environ.get("CAMPAIGN_CLIENTHUB_ID", "").strip()
TOK = os.environ.get("CAMPAIGN_CLIENTHUB_TOKEN", "").strip()

if not (CID and TOK):
    print("ERROR: creds missing"); sys.exit(1)

r = requests.get(f"{API_BASE}/api/campaigns/{CID}",
                 params={"access_token": TOK}, timeout=20)
data = r.json()

print("=== favorites ===")
print(json.dumps(data.get("favorites", {}), indent=2)[:10000])

print("\n\n=== _views_ ===")
print(json.dumps(data.get("_views_", {}), indent=2)[:10000])
