#!/usr/bin/env python

'''
This script gets the logs for a Job on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

- Set the variable JOB_ID at the top of the script for the Job to get metrics for

 
'''

import datetime,os,sys
from streamsets.sdk import ControlHub

# Job to get logs for
JOB_ID= 'dbdb9902-6cf4-45b1-91c4-a9977a2db4e6:8030c2e9-1a39-11ec-a5fe-97c8d4369386'

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

# Get the Job
job = None
try:
    job = sch.jobs.get(job_id = JOB_ID)
except:
    sys.exit('Error: Job with ID \'' + JOB_ID + '\' not found.')

print_message('Found Job with name \'' + job.job_name + '\'')

## Get the Job logs
job.refresh()
print(job.get_run_logs())

print_message('Done')
