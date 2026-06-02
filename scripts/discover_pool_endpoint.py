#!/usr/bin/env python3
"""Discovery probe v3: try header-auth + multiple hosts/endpoint shapes."""
import os, sys, requests

CID    = os.environ.get("CAMPAIGN_1_ID")
TOKEN  = os.environ.get("CAMPAIGN_1_TOKEN")
TENANT = os.environ.get("DIALFIRE_TENANT_ID")
TTOK   = os.environ.get("DIALFIRE_TENANT_TOKEN")
if not (CID and TOKEN and TENANT and TTOK):
    sys.exit("need CAMPAIGN_1_ID, CAMPAIGN_1_TOKEN, DIALFIRE_TENANT_ID, DIALFIRE_TENANT_TOKEN")

HOSTS = ["https://api.dialfire.com", "https://connect.dialfire.com", "https://clienthub.dialfire.com"]
PATHS = [
    f"/api/tenants/{TENANT}/campaigns",
    f"/api/tenants/{TENANT}/campaigns/{CID}",
    f"/api/tenants/{TENANT}/campaigns/{CID}/tasks/_count",
    f"/api/campaigns/{CID}",
    f"/api/campaigns/{CID}/tasks/_count",
    f"/api/v1/tenants/{TENANT}/campaigns/{CID}",
]
AUTH_SHAPES = [
    ("query_access_token", lambda t: ({"access_token": t}, {})),
    ("header_bearer",       lambda t: ({}, {"Authorization": f"Bearer {t}"})),
    ("header_token",        lambda t: ({}, {"X-Auth-Token": t})),
    ("header_apikey",       lambda t: ({}, {"X-Api-Key": t})),
]

def probe(label, token):
    print(f"\n========== {label} ==========")
    for host in HOSTS:
        for path in PATHS:
            for shape_name, shape_fn in AUTH_SHAPES:
                params, headers = shape_fn(token)
                url = host + path
                try:
                    r = requests.get(url, params=params, headers=headers, timeout=10, allow_redirects=False)
                    if r.status_code not in (403, 404):                       # only print interesting ones
                        body = r.text[:200].replace("\n", " ")
                        print(f"  [{r.status_code}] {shape_name:20s} {host} {path}  body: {body!r}")
                    elif r.status_code == 403 and "forbidden" not in r.text.lower():
                        # 403 with non-plain-text body might mean different
                        body = r.text[:120].replace("\n", " ")
                        print(f"  [403*] {shape_name:20s} {host} {path}  body: {body!r}")
                except Exception as e:
                    pass

probe("tenant_token", TTOK)
probe("campaign_token", TOKEN)
print("\ndone.")
