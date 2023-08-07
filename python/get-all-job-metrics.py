#!/usr/bin/env python

'''
This script gets metrics for all Jobs running
on StreamSets Platform except for Jobs that are in an INACTIVE state.
The script writes metrics to a rolling log file and can optionally 
print them to the console as well

Prerequisites:

 - Create a directory to hold the rolling log file

 - Python 3.6+; Python 3.9+ preferred

 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html

 - StreamSets Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables
   prior to running the script:

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>

'''

import json, os, sys, logging, time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from streamsets.sdk import ControlHub

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

# Whether or not to print metrics to the console
print_metrics_to_console = True

# How often to capture Job metrics
# Do not set this value to less than 5 minutes
metrics_capture_interval_seconds = 5 * 60 # every five minutes

# The directory and name for the rolling log file
output_dir = '</path/to/your/logging/dir>'
log_file_name = 'streamsets-job-metrics.log'

# Rolling Logfile config
log_file = output_dir + '/' + log_file_name
max_bytes_pre_log_file = 1024 * 1024 * 1024  # 1GB max log file size
number_of_rolling_logfiles = 5  # 5 rolling log files max

# Confirm the logging directory exists
if not os.path.isdir(output_dir):
    print('Error: the directory \'' + output_dir + '\' does not exist')
    print('Please create that directory in advance')
    sys.exit(-1)

# Method to create a rolling log file
def create_rotating_log():
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_file, maxBytes=max_bytes_pre_log_file, backupCount=number_of_rolling_logfiles)
    logger.addHandler(handler)
    return logger

# Method that returns a dictionary of sdc.id keys mapped to SDC URL values
def get_sdc_urls(data_collectors):
    sdc_urls = {}
    for sdc in data_collectors:
        sdc_urls[sdc.id] = sdc.engine_url
    return sdc_urls

# Method to get SDC UDL for sdc_id; return None is not found
def get_sdc_url(sdc_id):
    if sdc_id in sdc_urls.keys():
        return sdc_urls[sdc_id]
    else:
        return None

# Connect to Control Hub
print('Connecting to Control Hub')
sch = ControlHub(
    credential_id=CRED_ID,
    token=CRED_TOKEN)

# Populate the dictionary of sdc.id keys mapped to SDC URL values
sdc_urls = get_sdc_urls(sch.data_collectors)

# Create our rolling log file
logger = create_rotating_log()

while(True):

    start_time_seconds = time.time()

    # Get all Jobs that are not Inactive
    jobs = [job for job in sch.jobs if job.currentJobStatus['status'] != 'INACTIVE']

    ## Get metrics for each Job
    for job in jobs:
        job.refresh()
        metrics = None
        try:
            metrics = job.metrics
        except Exception as e:
            # No metrics exist for Job
            pass

        # Get Metrics
        if metrics is not None:
            data = {}
            data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data['job_name'] = job.job_name

            # Uncomment this line if you want to write JOB IDs as well
            # data['job_id'] = job.job_id

            # Get Job Status
            status = job.currentJobStatus
            data['job_status'] = status['status']
            data['job_status_color'] = status['color']
            data['job_warnings'] = status['warnings']
            data['job_error'] = status['errorInfos']

            # Get Pipeline Status
            pipeline_status = []
            for s in status['pipelineStatus']:
                pipeline_status.append({"sdc_url" : get_sdc_url(s['sdcId']), "status" : s['status'], "message": s['message']})

            data['pipeline_status'] = pipeline_status


            # Get Job Metrics
            metric = job.metrics[0]
            data['metrics_sdc_url'] = get_sdc_url(metric.sdc_id)
            data['job_run_count'] = metric.run_count
            data['job_input_count'] = metric.input_count
            data['job_output_count'] = metric.output_count

            # Get current Job offsets
            current_job_offsets = []
            for o in job.history[0].offsets:
                current_job_offsets.append(json.loads(o.offset))
            data['current_job_offsets'] = current_job_offsets

            # Convert the payload to JSON
            json_data = json.dumps(data)

            # Print metrics to the console if needed
            if print_metrics_to_console:
                print(json_data)

            # Write metrics to the rolling logfile
            logger.info(json_data)

    end_time_seconds = time.time()

    # Calculate how long to sleep before starting the metrics collciton loop again
    sleep_time_seconds = metrics_capture_interval_seconds - (end_time_seconds - start_time_seconds)

    if sleep_time_seconds > 0:
        time.sleep(sleep_time_seconds)

print('Done')