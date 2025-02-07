#!/usr/bin/python3

"""
FILE: export_job.py

DESCRIPTION: A script to import a StreamSets Job

ARGS:  job_archive_file

USAGE: $ python3 import_job.py <job_archive_file>

EXAMPLE USAGE: $ python3 import_job.py /Users/mark/data/jobs/Job_for_Weather_to_Snowflake.zip

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
        $ python3 import_job.py /Users/mark/data/jobs/Job_for_Weather_to_Snowflake.zip
          Connecting to Control Hub
          Importing archive '/Users/mark/data/jobs/Job_for_Weather_to_Snowflake.zip'
          Done
"""

import os
from streamsets.sdk import ControlHub
import sys

# Check the number of command line args
if len(sys.argv) != 2:
    print('Error: Wrong number of arguments')
    print('Usage: $ python3 import_job.py <job_archive_file>')
    sys.exit(1)

# Get command line args
archive_file = sys.argv[1]

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

# Import the archive
print('Importing archive \'{}\''.format(archive_file))
with open(archive_file, 'rb') as file:
    content = file.read()

sch.import_jobs(archive=content, pipeline=True, number_of_instances=True, labels=True, runtime_parameters=True)

print('Done')