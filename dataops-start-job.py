#!/usr/bin/env python

'''
This script starts a Job on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

- Set the variable JOB_ID at the top of the script for the Job to start

- Optionally, set the Job's runtime parameters
 
'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Job to start
JOB_ID= '8e4a0f7e-b752-4b05-8e94-c0e7055de643:8030c2e9-1a39-11ec-a5fe-97c8d4369386'

# Runtime Parameters (optional)
RUNTIME_PARAMETERS = {"PARAM_1": "aaa", "PARAM_2": "bbb"}

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

# How often to poll Control Hub for Job status
POLLING_FREQUENCY_SECONDS = 10

# How long to wait for a started Job to become active
MAX_WAIT_SECONDS_FOR_JOB_TO_BEOME_ACTIVE = 120

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

# Make sure Job has INACTIVE status
if job_status != 'INACTIVE':
    print_message('Error: Job must have status \'INACTIVE\' in order to be started')
    sys.exit(-1)

## Set the Job's Runtime Parameters
print_message('Setting Job parameters...')
job.runtime_parameters = RUNTIME_PARAMETERS
sch.update_job(job)

## Start the Job
print_message('Starting Job...')
sch.start_job(job)

## Wait for the Job to become Active
job.refresh()  
wait_seconds = 0
while job.status.status != 'ACTIVE':
    job.refresh()
    print_message('Waiting for Job to become ACTIVE...')
    sleep(POLLING_FREQUENCY_SECONDS)
    wait_seconds += POLLING_FREQUENCY_SECONDS
    if wait_seconds > MAX_WAIT_SECONDS_FOR_JOB_TO_BEOME_ACTIVE:
        # Exit if Job did not become ACTIVE within the specified time
        print_message('Error: Timeout waiting for Job to become ACTIVE')
        sys.exit(-1) 

print_message('Job status is ACTIVE')
print_message('Done')
