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
    Connected to SDC at http://localhost:18888
    Found pipeline 'Weather Raw to Refined'
    Pipeline input record count: 6683
    Pipeline output record count: 6683
    Time of Last Record Received: 2023-12-20 15:44:41.396000

'''

# Imports
import sys
import datetime
from streamsets.sdk import ControlHub

# Control Hub API credentials
cred_id = ''
cred_token = ''

# SDC URL
sdc_url = ''

# Pipeline name
pipeline_name = 'Weather Raw to Refined'

# Set to True if WebSocket Communication is enabled
# Set to False if Direct REST APIs are used
websockets_enabled = True

# Connect to Control Hub
sch = None
try:
    sch = ControlHub(credential_id=cred_id, token=cred_token, use_websocket_tunneling=websockets_enabled)
except Exception as e:
    print('Error: Could not connect to Control Hub.')
    print('Exception: ' + str(e))
    sys.exit(-1)

# Connect to the Data Collector
sdc = None
try:
    sdc = sch.data_collectors.get(engine_url=sdc_url)._instance
except Exception as e:
    print('Error: Could not connect to Data Collector')
    print('Error; ' + str(e))
    sys.exit(-1)

print('Connected to SDC at {}'.format(sdc_url))

# Get pipeline
pipeline = None
try:
    pipeline = sdc.pipelines.get(title=pipeline_name)
except Exception as e:
    print('Error: Could not find pipeline')
    print('Error; ' + str(e))
    sys.exit(-1)

print('Found pipeline \'{}\''.format(pipeline_name))

pipeline_metrics = sdc.get_pipeline_metrics(pipeline).pipeline

# Pipeline Metrics
print('Pipeline input record count: {}'.format(pipeline_metrics.input_record_count))
print('Pipeline output record count: {}'.format(pipeline_metrics.output_record_count))

# Time of Last Record Received
gauges = pipeline_metrics._data['gauges']
runtime_stats_gauge = gauges['RuntimeStatsGauge.gauge']
millis = runtime_stats_gauge['value']['timeOfLastReceivedRecord']
time_of_last_record_received = datetime.datetime.fromtimestamp(millis/1000.0)
print('Time of Last Record Received: {}'.format(time_of_last_record_received))


