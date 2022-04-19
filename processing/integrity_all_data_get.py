# This script gets all operation data in an xml file from the Organic Integrity API.
# Set to work with windows (doubled backslashes)

import requests
import os
import shutil
from zipfile import ZipFile
import sys
import datetime
import csv

# Retrieve api keys
def key_get(key_name, file = "C:\\Users\\daniel\\Documents\\organic_env\\authentication\\api_keys.csv"):
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

# Delete folders from current directory containing folder_name variable
# if there are more than 5. Set to delete based on sorting by file name - works with standardized timestamp suffix.
def backup_manage(folder_name, backup_num):
    file_list = list()
    for i in os.listdir(os.getcwd()):
        if folder_name in i:
            file_list.append(i)
            
    if len(file_list) > backup_num:
        file_list.sort(reverse=True)

        for i in file_list[backup_num:]:
            shutil.rmtree(os.getcwd() + "\\" + i)


# File path for holding download and xml file.
dwnld_name = "integrity_download"
dwnld_dir = os.getcwd() + "\\" + dwnld_name

relace_flag = False

# If this data was retrieved, rename its directory with a timestamp. Make a new directory with the base name in dwnld_dir.
if dwnld_name in os.listdir(os.getcwd()):
    # Get timestamp to mark currently existing directory, if any.
    timestamp = datetime.datetime.strftime(datetime.datetime.today(),'%Y-%m-%d %H-%M-%S')
    # Rename the existing directory with the timestamp
    os.rename(dwnld_dir, dwnld_dir + " replaced " + timestamp)

    # Make sure more than 5 of these backups are not maintained.
    backup_manage(dwnld_name, 5)
    
    # Mark if the name change was done, in case of exceptions.
    replace_flag = True
    
os.makedirs(dwnld_dir)

# Get the xml data from the Integrity API.
MY_KEY = key_get("integrity")
end_point = "https://organicapi.ams.usda.gov/IntegrityPubDataServices/OidPublicDataService.svc/rest/GetAllOperationsPublicData?api_key=" + MY_KEY
integrity_zip = dwnld_dir + "\\" + "integrity_xml.zip"

try:
    # From https://365datascience.com/tutorials/python-tutorials/python-requests-package/
    with requests.get(end_point, stream=True) as r:
        with open(integrity_zip, "wb") as file:
            for chunk in r.iter_content(chunk_size = 16 * 1024):
                file.write(chunk)
                
except requests.ConnectionError:
    print("A connection error occured!")

    # Remove the new download directory.
    shutil.rmtree(dwnld_dir)

    # If there was an existing download directory that was renamed with a timestamp, change the directory name back.
    if replace_flag:
        os.rename(dwnld_dir + " replaced " + timestamp, dwnld_dir)
        
    sys.exit()

# Extract from the downloaded ZIP file.
with ZipFile(integrity_zip, "r") as z:
    z.extractall(dwnld_dir)

# Delete the zip file after extraction.
os.remove(integrity_zip)

# Contents are downloading without file extension. If this is the case add the xml extension.
for i in os.listdir(dwnld_dir):
    if i == "stream":
        os.rename(dwnld_dir + "\\" + i, dwnld_dir + "\\" + "stream.xml")
        
