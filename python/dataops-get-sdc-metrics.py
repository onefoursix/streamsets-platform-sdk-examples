#!/usr/bin/env python

'''
This script gets metrics for a Data Collectors on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>
 
'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

# print_message method which writes a timestamp message ot the console
def print_message(message):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' ' +   message)

# Connect to Control Hub
print_message('Connecting to Control Hub')
sch = ControlHub(
    credential_id=CRED_ID, 
    token=CRED_TOKEN)

# Get SDCs
sdcs = sch.data_collectors
for sdc in sdcs:
    print('----------')
    print('SDC URL: ' + sdc.engine_url)
    #help(sdc)
    print('SDC CPU LOAD: ' + str(sdc.cpu_load))
    print('SDC MEMORY (MB): ' + str(sdc.memory_used_mb))
    print('SDC Running Pipelines Count: ' + str(sdc.running_pipelines_count))
    print('SDC Running Pipelines: ' + str(sdc.running_pipelines))
    print('----------')

print_message('Done')
