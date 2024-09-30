"""
get_streamsets_oracle_cdc_lag_metrics.py

This script retrieves StreamSets Oracle CDC lag metrics for a running Job.

The script will run forever until stopped.

Set the sleep_time_seconds to control the refresh frequency

Prerequisites
-------------
- Python 3.9+ (tested with Python 3.11.5)

- StreamSets API Credentials set in the environment prior to running the script.
  For example:
    $ export CRED_ID="zzzz"
    $ export CRED_TOKEN="zzzz"

- The StreamSets SDK for Python v6.0+ (tested with v6.4)


Command Line Arguments
----------------------
The script requires one command line arguments:

- job_id - The Job ID for a running Oracle CDC Job



Example Usage
-------------
$ python3 get_streamsets_oracle_cdc_lag_metrics.py 35c675ae-665b-4129-b22b-2bf8e491f197:8030c2e9-1a39-11ec-a5fe-97c8d4369386


Sample Output
-------------

$ python3 get_streamsets_oracle_cdc_lag_metrics.py 35c675ae-665b-4129-b22b-2bf8e491f197:8030c2e9-1a39-11ec-a5fe-97c8d4369386
-------------------------------------
Connected to Control Hub
-------------------------------------
Found Job 'Oracle CDC to Snowflake (new origin)'
-------------------------------------
2024-09-30 16:19:16 Oracle CDC Lag metric: 59 seconds
2024-09-30 16:19:48 Oracle CDC Lag metric: 14 seconds
2024-09-30 16:20:20 Oracle CDC Lag metric: 19 seconds
2024-09-30 16:20:51 Oracle CDC Lag metric: 23 seconds
2024-09-30 16:21:23 Oracle CDC Lag metric: 54 seconds
2024-09-30 16:21:55 Oracle CDC Lag metric: 28 seconds
...
"""

import os
from datetime import datetime
from time import sleep
import sys
from streamsets.sdk import ControlHub

# How long to sleep between calls to get the metrics
sleep_time_seconds = 30

def print_usage_and_exit():
    print('Usage: $ python3 get_streamsets_oracle_cdc_lag_metrics.py <job_id>')
    sys.exit(1)

def print_message(message):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' ' +   message)

def get_oracle_cdc_lag_metric():
    lag_metric = None

    # Oracle CDC Lag Time metric name for the new Oracle CDC Origin
    new_oracle_cdc_lag_metric_key = 'custom.OracleCDC_1.Summary 02 - Latency.0.gauge'

    # Oracle CDC Lag Time metric name for the old Oracle CDC Client Origin
    old_oracle_cdc_lag_metric_key = 'custom.OracleCDCClient_1.Work State A: RedoLog Archives.0.gauge'

    try:
        # noinspection PyProtectedMember
        gauges = job.realtime_summary._data['gauges']

        # Get the appropriate lag metric for the old or new Oracle CDC origin
        if new_oracle_cdc_lag_metric_key in gauges.keys():
            lag_metric = gauges[new_oracle_cdc_lag_metric_key]['value']['Server Instant Latency']
        elif old_oracle_cdc_lag_metric_key in gauges.keys():
            lag_metric = gauges[old_oracle_cdc_lag_metric_key]['value']['Read lag (seconds)']

    except Exception as ex:
        print_message('Error getting Oracle CDC lag metric: ' + str(ex))

    return lag_metric



# Get Control Hub Credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')
if cred_id is None or len(cred_id) == 0:
    print('Error: CRED_ID is not set in the environment')
    exit(1)
if cred_token is None or len(cred_token) == 0:
    print('Error: CRED_TOKEN is not set in the environment')
    exit(1)

# Check the number of command line args
if len(sys.argv) != 2:
    print('Error: Wrong number of arguments')
    print_usage_and_exit()

# Get the Job ID from the command line
job_id = sys.argv[1]

# Connect to Control Hub
sch = None
try:
    sch = ControlHub(credential_id=cred_id, token=cred_token)
except Exception as e:
    print('Error connecting to Control Hub')
    print(str(e))
    sys.exit(1)
print('-------------------------------------')
print('Connected to Control Hub')
print('-------------------------------------')

# Find the Job
try:
    job = sch.jobs.get(job_id=job_id)
except:
    print('Error: Could not find Job for Job ID \'{}\''.format(job_id))
    exit(1)
print('Found Job \'{}\''.format(job.job_name))
print('-------------------------------------')


# Check metrics in an infinite loop until this script is stopped
while True:
    job.refresh()
    job_status = job.history[0].status
    if job_status != 'ACTIVE':
        print_message('Job status is \'{}\'. Oracle CDC lag metrics will be gathered once the Job is ACTIVE'.format(job_status))
    else:
        print_message('Oracle CDC Lag metric: {}'.format(get_oracle_cdc_lag_metric()))
    sleep(sleep_time_seconds)


