#!/usr/bin/env python

'''
This script promotes a piepline version for a Job on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>
 
- Set the Job ID
- Set the pipeline version to promote to the Job

'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Job ID
JOB_ID= '<your-job-id>'

# Pipeline version to set for the Job
PIPELINE_VERSION= '<new version of pipeline>'

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
#help(job)


# Get the pipeline associated with the Job
pipeline = sch.pipelines.get_all(commit_id=job.commit_id)[0]


print_message('Job\'s pipeline name: ' + pipeline.name)
print_message('Job\'s current pipeline version: ' + pipeline.version + '\'')
print_message('Looking for pipeline version \'' + PIPELINE_VERSION + '\' of pipeline \'' + pipeline.name + '\'...')

try:
    new_version_of_pipeline = sch.pipelines.get_all(pipeline_id=job.pipeline_id, version=PIPELINE_VERSION)[0]
except:
    print_message('Error: Version \'' + PIPELINE_VERSION + '\' not found for pipeline \'' + pipeline.name + '\'')
    sys.exit(-1)

print_message('Found version \'' + PIPELINE_VERSION + '\' of pipeline \'' + pipeline.name + '\'')

print_message('Upgrading Job to version \'' + PIPELINE_VERSION + '\' of pipeline \'' + pipeline.name + '\'')

job.commit_id = new_version_of_pipeline.commit_id
sch.upgrade_job(job)

print_message('Done')
