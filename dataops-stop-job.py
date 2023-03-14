#!/usr/bin/env python

'''
This script stops a Job on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

- Set the variable JOB_ID at the top of the script for the Job to stop
 
'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Job to start
JOB_ID= '<your-job-id>'

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

# How often to poll Control Hub for Job status
POLLING_FREQUENCY_SECONDS = 10

# How long to wait for a stopped Job to become inactive
MAX_WAIT_SECONDS_FOR_JOB_TO_BEOME_INACTIVE = 5 * 60 # Five minutes

# print_message method which writes a timestamp message ot the console
def print_message(message):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' ' +   message)

# Connect to Control Hub
print_message('Connecting to Control Hub')
sch = ControlHub(
    credential_id=CRED_ID, 
    token=CRED_TOKEN)

## Get the Job
job = None
try:
    job = sch.jobs.get(job_id = JOB_ID)
except:
    sys.exit('Error: Job with ID \'' + JOB_ID + '\' not found.')

print_message('Found Job with name \'' + job.job_name + '\'')

## Get the Job status
job.refresh()
job_status = job.status.status
print_message('Job status is \'' + job_status + '\'')

# Make sure Job has ACTIVE status
if job_status != 'ACTIVE':
    print_message('Error: Job must have status \'ACTIVE\' in order to be stopped')
    sys.exit(-1)

## Stop the Job
print_message('Stopping Job...')
sch.stop_job(job)

## Wait for the Job to become inactive
job.refresh()  
wait_seconds = 0
while job.status.status != 'INACTIVE':
    job.refresh()
    print_message('Waiting for Job to become INACTIVE...')
    sleep(POLLING_FREQUENCY_SECONDS)
    wait_seconds += POLLING_FREQUENCY_SECONDS
    if wait_seconds > MAX_WAIT_SECONDS_FOR_JOB_TO_BEOME_INACTIVE:
        # Exit if Job did not become ACTIVE within the specified time
        print_message('Error: Timeout waiting for Job to become INACTIVE')
        sys.exit(-1) 

print_message('Job status is INACTIVE')
print_message('Done')
