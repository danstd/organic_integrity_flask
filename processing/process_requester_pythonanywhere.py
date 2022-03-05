# This file sends post requests to routes in the flask integrity_app to re-process data as part of a workflow to be done periodically after
# new data is received from the organic integrity api.
import requests
from api_key_get import key_get

KEY = key_get("integrity_app_process")
headers = {"key": KEY}

# Process world view data.
url = "http://127.0.0.1:5000/world_process"
res = requests.post(url, headers=headers)
if res.status_code != 200:
    print("Unsuccessful request for World view")
else: print(res.text)

# Process US view data.
url = "http://127.0.0.1:5000/us_process"
res = requests.post(url, headers=headers)
if res.status_code != 200:
    print("Unsuccessful request for U.S. view")
else: print(res.text)

# Process Products view data.
url = "http://127.0.0.1:5000/products_process"
res = requests.post(url, headers=headers)
if res.status_code != 200:
    print("Unsuccessful request for Products view")
else: print(res.text)
