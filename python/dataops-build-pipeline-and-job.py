#!/usr/bin/env python

'''
This script builds a Pipeline and Job on StreamSets DataOps Platform 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>
 
- Set a Data Collector URL variable below

'''


import datetime,os,sys
from streamsets.sdk import ControlHub

# Data Collector URL
SDC_URL= '<your SDC URL>'

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

# Get the SDC
try:
    sdc = sch.data_collectors.get(engine_url=SDC_URL)
except:
    print_message('Error getting SDC with URL \''  + SDC_URL + '\'')
    sys.exit(-1)

# Get a pipeline builder
pipeline_builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id=sdc.id)


# Here is a trivial example of a pipeline
dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
trash = pipeline_builder.add_stage('Trash')
dev_raw_data_source >> trash


# See the section below for a more complex pipeline

'''
# Add a JDBC Multi-table origin
jdbc_origin = pipeline_builder.add_stage('JDBC Multitable Consumer')
jdbc_origin.jdbc_connection_string = 'jdbc:mysql://warsaw:3306/claims'
jdbc_origin.use_credentials = True
jdbc_origin.username = 'mark'
jdbc_origin.password = 'mark'
jdbc_origin.table_configs[0]['schema'] = 'claims'
jdbc_origin.table_configs[0]['tablePattern'] = 'CLAIMS_%'

# Add a Field Masker Processor
field_masker = pipeline_builder.add_stage('Field Masker')
field_masker.field_mask_configs[0]['fields']= ['/L_NAME','/F_NAME']
field_masker.field_mask_configs[0]['maskType'] = 'FIXED_LENGTH'

# Add a Kafka Producer
kafka = pipeline_builder.add_stage('Kafka Producer')
kafka.library = "streamsets-datacollector-apache-kafka_3_3-lib"
kafka.broker_uri = "portland:9092,chicago:9092,brooklyn:9092"
kafka.topic = "claims"

## Create links between stages
jdbc_origin >> field_masker >> kafka

# Error handling
pipeline_builder.add_error_stage('Discard')
'''

# Build the pipeline and add it to Control Hub
pipeline = pipeline_builder.build('SDK-Pipeline')

print_message('Adding pipeline to Control Hub')
sch.publish_pipeline(pipeline, commit_message='First commit of SDK-Pipeline', draft=False)

# Get a job builder
job_builder = sch.get_job_builder()

# Get the pipeline commit
pipeline_commit = pipeline.commits.get(version='1')

# Create a job
job = job_builder.build('Job for SDK Pipeline', pipeline=pipeline, pipeline_commit=pipeline_commit)

# Add the Data Ceollector labels to the job
job.data_collector_labels = ['DEV']

# Add the job to Control Hub 
print_message('Adding Job to Control Hub')
sch.add_job(job)

print_message('Done')
