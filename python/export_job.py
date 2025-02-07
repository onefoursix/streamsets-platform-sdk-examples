#!/usr/bin/python3

"""
FILE: export_job.py

DESCRIPTION: A script to export a StreamSets Job

ARGS:  job_id
       export_dir

USAGE: $ python3 export_job.py <job_id> <export-dir>

EXAMPLE USAGE: $ python3 export_job.py 12ba1a0f-997c-4a8b-954c-66ab6be2a565:8030c2e9-1a39-11ec-a5fe-97c8d4369386 /Users/mark/data/jobs


PREREQUISITES:

 - Python 3.9+

 - StreamSets Platform SDK for Python v6.5+
   See: https://docs.streamsets.com/platform-sdk/latest/welcome/installation.html

 - StreamSets Platform API Credentials for a user with Organization Administrator role

- Before running the script, export the environment variables CRED_ID and CRED_TOKEN
  with the StreamSets Platform API Credentials, like this:

    $ export CRED_ID="40af8..."
    $ export CRED_TOKEN="eyJ0..."

- Here is sample output from running the script:

        $ export CRED_ID="xxxxxxxxxx"
        $ export CRED_TOKEN="xxxxxxxxxx"
        $ python3 export_job.py '12ba1a0f-997c-4a8b-954c-66ab6be2a565:8030c2e9-1a39-11ec-a5fe-97c8d4369386' '/Users/mark/data/jobs'
          Connecting to Control Hub
          Found Job 'Job for Weather to Snowflake'
          Exported the file '/Users/mark/data/jobs/Job_for_Weather_to_Snowflake.zip'
          Done
"""

import os
from streamsets.sdk import ControlHub
import sys

# mkdir method
def mkdir(the_dir):
    if not os.path.exists(the_dir):
        os.mkdir(the_dir)


# export_resource method
def export_resource(export_dir, resource_name, data):
    # replace '/' with '_' in resource name
    resource_name = resource_name.replace("/", "_")
    resource_name = resource_name.replace(" ", "_")

    # Export a zip file for the resource
    file_name = export_dir + '/' + resource_name + '.zip'
    with open(file_name, 'wb') as file:
        file.write(data)
        print('Exported the file \'{}\''.format(file_name))


# Check the number of command line args
if len(sys.argv) != 3:
    print('Error: Wrong number of arguments')
    print('Usage: $ python3 export_job.py <job_id> <export-dir>')
    sys.exit(1)

# Get command line args
job_id = sys.argv[1]
output_dir = sys.argv[2]

# Get Control Hub Credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')

# Connect to Control Hub
sch = None
print('Connecting to Control Hub')
try:
    sch = ControlHub(credential_id=cred_id, token=cred_token)
except Exception as e:
    print('Error connecting to Control Hub; check your CRED_ID and CRED_TOKEN environment variables')
    print(str(e))
    sys.exit(1)

# Make the output dir
mkdir(output_dir)

# Get the Job
the_job = None
for job in sch.jobs:
    if job.job_id == job_id:
        the_job = job
        break

if the_job is not None:
    print('Found Job \'{}\''.format(the_job.job_name))
else:
    print('Error: could not find Job for job_id: \'{}\''.format(job_id))
    sys.exit(1)

# Export the Job
data = sch.export_jobs([the_job])
export_resource(output_dir, the_job.job_name, data)

print('Done')