#!/usr/bin/env python

'''
This script sets labels on one or more StreamSets Engines 
at the engine level as opposed to at the deployment level.

Engine labels set at the engine level are immediately
visible on the engine, but will not survive if an SDC Pod 
dies and is recreated by Kubernetes

Engine labels set at the deployment level are not
visible on the engine until the deployment is restarted,
though these labels will survive if an SDC Pod 
dies and is recreated by Kubernetes.



 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - StreamSets Platform API Credentials for a user with permissions to update engines

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

- Set these variables at the top of the script:

    -  TODO TOTDO TODO

    Set the list of ENGINE_IDS to the list of Engines to update
    - Optionally, set the Job's runtime parameters
 
'''


import datetime,os,sys
from streamsets.sdk import ControlHub
from time import sleep

# list of engines?


# list of deployments

# List of labels to set
LABELS_TO_ADD = ['new_label_1', 'new_label_2']
LABELS_TO_REMOVE = ['old_label_1', 'old_label_2']

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

# How often to poll Control Hub for Job status updates
POLLING_FREQUENCY_SECONDS = 10

# How long to wait for the Job to become Active
MAX_WAIT_SECONDS_FOR_JOB_TO_BECOME_ACTIVE = 20

# How long to wait for the Job to become Inactive
MAX_WAIT_SECONDS_FOR_JOB_TO_BECOME_INACTIVE = 5 * 60 # 5 minutes

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

## Get the Job status
job.refresh()
job_status = job.status.status
print_message('Job status is \'' + job_status + '\'')

# Make sure Job has either ACTIVE or INACTIVE status
if job_status not in ('ACTIVE','INACTIVE'):
    print_message('Error: Job must have status of either \'ACTIVE\' or \'INACTIVE\' for this script to run')
    sys.exit(-1)

# Stop the Job if it is active
if job_status == 'ACTIVE':
    print_message('Attempting to stop Job...')
    try:
        sch.stop_job(job)
    except:
        print_message('Error occurred while trying to stop the Job') 
        sys.exit(-1) 
        
    while job_status != 'INACTIVE':
        wait_seconds = 0
        job.refresh()
        job_status = job.status.status
        print_message('Waiting for Job to become INACTIVE')

        if wait_seconds > MAX_WAIT_SECONDS_FOR_JOB_TO_BECOME_INACTIVE:
            print_message('Error: Timeout waiting for Job to become INACTIVE') 
            sys.exit(-1) 
        
        sleep(POLLING_FREQUENCY_SECONDS)
        wait_seconds += POLLING_FREQUENCY_SECONDS
        
    print_message('Job status is INACTIVE')

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
