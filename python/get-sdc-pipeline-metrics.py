#!/usr/bin/env python

'''
Example of how to get pipeline metrics for a running Job 
directly from an SDC on StreamSets Platform as well as the
time of last record received

Set the following variables in the script:

    - cred_id
    - cred_token
    - sdc_url
    - job_name

Sample output looks like this:

    $ python get-sdc-pipeline-metrics.py
    Connected to Control Hub
    Found Job 'Job for Weather Raw to Refined'
    Connected to SDC at http://localhost:18888
    Found pipeline 'Weather Raw to Refined'
    Pipeline input record count: 1419
    Pipeline output record count: 1419
    Time of Last Record Received: 2023-12-21 11:08:48.629000

'''

# Imports
import sys
from datetime import datetime
from streamsets.sdk import ControlHub

# Control Hub API credentials
cred_id = ''
cred_token = ''

# SDC URL
sdc_url = ''

# Job name
job_name = ''

# Set to True if WebSocket Communication is enabled
# Set to False if Direct REST APIs are used
websockets_enabled = True

# Connect to Control Hub
sch = None
try:
    sch = ControlHub(credential_id=cred_id, token=cred_token, use_websocket_tunneling=websockets_enabled)
except Exception as e:
    print('Error: Could not connect to Control Hub.')
    print(str(e))
    sys.exit(-1)

print('Connected to Control Hub')

# Get the Job
job = None
try:
    job = sch.jobs.get(job_name=job_name)
except Exception as e:
    print('Error: Could not find Job \'{}\''.format(job_name))
    print(str(e))
    sys.exit(-1)

print('Found Job \'{}\''.format(job_name))

# Connect to the Data Collector
sdc = None
try:
    sdc = sch.data_collectors.get(engine_url=sdc_url)._instance
except Exception as e:
    print('Error: Could not connect to Data Collector')
    print(str(e))
    sys.exit(-1)

print('Connected to SDC at {}'.format(sdc_url))


# Get pipeline
pipeline = None
try:
    pipelines = sdc.pipelines
    for p in pipelines:
        if p.parameters['JOB_ID'] == job.job_id:
            pipeline = p
            print('Found pipeline \'{}\''.format(p.title))
            break
    if pipeline is None:
        print('Error: Could not find pipeline for Job \'{}\''.format(job_name))
        sys.exit(-1)
except Exception as e:
    print('Error: Could not find pipeline for Job \'{}\''.format(job_name))
    print(str(e))
    sys.exit(-1)



pipeline_metrics = sdc.get_pipeline_metrics(pipeline).pipeline

# Pipeline Metrics
print('Pipeline input record count: {}'.format(pipeline_metrics.input_record_count))
print('Pipeline output record count: {}'.format(pipeline_metrics.output_record_count))

# Time of Last Record Received
gauges = pipeline_metrics._data['gauges']
runtime_stats_gauge = gauges['RuntimeStatsGauge.gauge']
millis = runtime_stats_gauge['value']['timeOfLastReceivedRecord']
time_of_last_record_received = datetime.fromtimestamp(millis/1000.0)
print('Time of Last Record Received: {}'.format(time_of_last_record_received))
