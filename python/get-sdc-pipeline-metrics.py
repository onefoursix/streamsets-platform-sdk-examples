#!/usr/bin/env python

'''
Example of how to get pipeline metrics for a running Job 
directly from an SDC on StreamSets Platform.

Set the following variables in the script:

    - cred_id
    - cred_token
    - sdc_url
    - job_name

Sample output looks like this:

    $ python get-sdc-pipeline-metrics.py
    Connected to Control Hub
    Connected to SDC at http://localhost:18888
    Found pipeline for Job 'Weather Raw to Refined'
    ---
    Pipeline input record count: 1775
    Pipeline output record count: 1775
    ---

Uncomment the last line of the script to print additional metrics

'''

# Imports
import sys
from streamsets.sdk import ControlHub


# Set your Control Hub API credentials
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
    pipeline = sdc.pipelines.get(title=job_name)
except Exception as e:
    print('Error: Could not find pipeline for Job \'{}\''.format(job_name))
    print(str(e))
    sys.exit(-1)

print('Found pipeline for Job \'{}\''.format(job_name))

pipeline_metrics = sdc.get_pipeline_metrics(pipeline).pipeline

print('---')
print('Pipeline input record count: {}'.format(pipeline_metrics.input_record_count))
print('Pipeline output record count: {}'.format(pipeline_metrics.output_record_count))
print('---')

# Uncomment this line to print additional metrics
# print(str(pipeline_metrics._data))
