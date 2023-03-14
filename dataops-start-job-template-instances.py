#!/usr/bin/env python

'''
This script starts multiple instances of a Job Template on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

- Set the variable JOB_ID at the top of the script for the Job Template to start instances

- Set an array of parameter values for the Job Template instances' runtime parameters
 
'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Job Template to start instances for
JOB_ID= '<your-job-template-id>'

# Job Template Instances Runtime Parameters
RUNTIME_PARAMETERS = [{'PARAM_1': 'aaa', 'PARAM_2': '111'}, {'PARAM_1': 'bbb', 'PARAM_2': '222'}]

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

# Get the Job Template
job_template = None
try:
    job_template = sch.jobs.get(job_id = JOB_ID)
except:
    sys.exit('Error: Job Template with ID \'' + JOB_ID + '\' not found.')

print_message('Found Job Template with name \'' + job_template.job_name + '\'')

# Create and start the Job Template Instances

print_message('Starting ' + str(len(RUNTIME_PARAMETERS)) + ' Job Template Instances')

job_template_instances = sch.start_job_template(job_template,
                              instance_name_suffix='PARAM_VALUE',
                              parameter_name='PARAM_1',
                              runtime_parameters=RUNTIME_PARAMETERS,
                              attach_to_template=True, 
                              delete_after_completion=False)



print_message('Done')
