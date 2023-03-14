#!/usr/bin/env python

'''
This script exports Fragments, Pipelines, Jobs, and Job Templates from StreamSets DataOps Platform
 
The current version of this script does not export Connections, Tasks, nor Topologies
 
 
Prerequisites:
 - Python 3.6+; Python 3.9+ preferred
 
 - StreamSets DataOps Platform SDK for Python v5.1+
   See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html
   
 - DataOps Platform API Credentials for a user with Organization Administrator role

 - To avoid including API Credentials in the script, export these two environment variables 
   prior to running the script: 

        export CRED_ID=<your CRED_ID>>
        export CRED_TOKEN=<your CRED_TOKEN>
 
'''


import os,sys
from streamsets.sdk import ControlHub

## User Variables ##################

# Get CRED_ID from the environment
CRED_ID = os.getenv('CRED_ID')

# Get CRED_TOKEN from the environment
CRED_TOKEN = os.getenv('CRED_TOKEN')

EXPORT_BASE_DIR = '<your-export-dir>'

####################################

# Child export dirs
FRAGMENTS_DIR = 'fragments'
PIPELINES_DIR = 'pipelines'
JOBS_DIR = 'jobs'
JOB_TEMPLATES_DIR = 'job-templates'


# Create the export directory if it does not exist
if not os.path.exists(EXPORT_BASE_DIR):
    try:
        os.mkdir(EXPORT_BASE_DIR)
    except Exception as err:
         print('Error creating export directory: ' + str(err))
         sys.exit(-1)

print('\nExporting resources to ' + EXPORT_BASE_DIR)

# Connect to Control Hub
sch = ControlHub(
    credential_id=CRED_ID, 
    token=CRED_TOKEN)

# print header method
def print_header(header):
    divider = 40 * '-'
    print('\n\n' + divider)
    print(header)
    print(divider)
    
# mkdir method
def mkdir(dir_to_create):
    path =   os.path.join(EXPORT_BASE_DIR, dir_to_create)
    if not os.path.exists(path):
        os.mkdir(path)

# export_resource method
def export_resource(export_dir, resource_name, data):

    # replace '/' with '_' in resource name
    resource_name = resource_name.replace("/", "_" )

    # Export a zip file for the resource
    with open(EXPORT_BASE_DIR + '/' + export_dir + '/' + resource_name + '.zip', 'wb') as file:
        file.write(data)
        
# export_pipelines_or_fragments method
def export_pipelines_or_fragments(resource_type, resources):

    if resource_type == 'pipeline':
        resource_label = 'Pipeline'
        export_dir = PIPELINES_DIR
    else:
        resource_label = 'Fragment'
        export_dir = FRAGMENTS_DIR

    for resource in resources:

        # Can't export a V1-DRAFT version as no commits exist
        if resource.version.endswith('1-DRAFT'):
            print(resource_label + ' \'' + resource.name + '\' version \'' + resource.version +  '\' has no committed versions and will not be exported.\n' )

        # If the version is a DRAFT with at least one commit, export the most recent commit instead of the DRAFT
        elif resource.version.endswith('DRAFT'):
            commits = resource.commits
            num_commits = len(commits)
            latest_commit = commits[num_commits - 1] 
            print(resource_label + ' \'' + resource.name + '\' version \'' + resource.version + '\' will not be exported because it is a DRAFT version.' )
            print('--> Exporting ' + resource_label + ' \'' + resource.name + ' version \'' + latest_commit.version + '\' instead.\n' )
            data = sch.export_pipelines([latest_commit.pipeline], fragments=True, include_plain_text_credentials=False)   
            export_resource(export_dir, latest_commit.pipeline.name, data)
        
        # If not a DRAFT, export the current version
        else:
            print('Exporting ' + resource_type + '\'' + resource.name + '\' version \'' + resource.version + '\'\n' )
            data = sch.export_pipelines([resource], fragments=True, include_plain_text_credentials=False)   
            export_resource(export_dir, resource.name, data)

            
# Fragments
print_header('Exporting Fragments')
mkdir(FRAGMENTS_DIR) 
fragments = sch.pipelines.get_all(fragment=True) 
export_pipelines_or_fragments('fragment', fragments)


# Pipelines
print_header('Exporting Pipelines')
mkdir(PIPELINES_DIR) 
pipelines = sch.pipelines
export_pipelines_or_fragments('pipeline', pipelines)


# Jobs
print_header('Exporting Jobs')
mkdir(JOBS_DIR) 
jobs = [job for job in sch.jobs if not job.job_template and not job.template_job_id]
for job in jobs:
    data = sch.export_jobs([job])
    print('Exporting Job \'' + job.job_name + '\'\n' )
    export_resource(JOBS_DIR, job.job_name, data)


# Job Templates
print_header('Exporting Job Templates')
mkdir(JOB_TEMPLATES_DIR) 
job_templates = [job for job in sch.jobs if job.job_template]
for job_template in job_templates:
    data = sch.export_jobs([job_template])
    print('Exporting Job Template \'' + job_template.job_name + '\'\n' )
    export_resource(JOB_TEMPLATES_DIR, job_template.job_name, data)
        
print('Done')