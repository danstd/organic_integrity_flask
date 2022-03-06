# This file sends post requests to routes in the flask integrity_app to re-process data as part of a workflow to be done periodically after
# new data is received from the organic integrity api.
import requests
import csv

def key_get(key_name, file = "C:\\Users\\daniel\\Documents\\organic_env\\api_keys.csv"):
    # Attempt to read existing file
    result_dict = dict()
    try:
        with open(file, "r", newline="") as csvObj:
            csvReader = csv.reader(csvObj)

            for row in csvReader:
                result_dict[row[0]] = row[1]

        return(result_dict[key_name])     
            
    except IOError:
        print("The api key file could not be read!")

KEY = key_get("integrity_app_process")
headers = {"key": KEY}

# Process Products view data.
url = "https://ddavis11.pythonanywhere.com/products_process"
res = requests.post(url, headers=headers)
if res.status_code != 200:
    print("Unsuccessful request for Products view")
else: print(res.text)