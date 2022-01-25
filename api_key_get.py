import json
import csv
    
def key_get(key_name, file = "api_keys.csv"):
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

	
if __name__ == "__main__":
    with open("api_keys.csv") as api:
        api_keys = json.load(api)
        print(api_keys)
