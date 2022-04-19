# From https://stackoverflow.com/questions/4408714/execute-sql-file-with-python-mysqldb
# Use mysql command line to read in data.

import json
from subprocess import Popen, PIPE
AUTH_FILE = "C:\\Users\\daniel\\Documents\\organic_env\\authentication\\flaskupdater_authentication.json"
SCRIPT_FILE = "C:/Users/daniel/Documents/organic_env/organic_integrity_flask/processing/integrity_read_in_local.sql"
USER = "flaskupdater"
PASSWD = "VOI!dS8g"

with open(AUTH_FILE) as auth:
    auth = json.load(auth)
    USER = auth["user"]
    PASSWD = auth["password"]

USER = 'root'
PASSWD = 'zlt75IF150'
#process = Popen(['mysql', "organic_integrity", '-u', USER, '-p', PASSWD],
#                stdout=PIPE, stdin=PIPE, shell=True)
#output = process.communicate('source ' + SCRIPT_FILE)[0]


command = ['mysql','-u',  USER, '-p', PASSWD]

with open(SCRIPT_FILE) as input_file:

    proc = Popen(command, stdin = input_file, stderr=PIPE, stdout=PIPE)
    output,error = proc.communicate()
    #print(output)
    #print(error)