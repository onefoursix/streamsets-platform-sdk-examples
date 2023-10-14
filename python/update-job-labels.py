#!/usr/bin/env python

'''
This script updates Job labels. 

Note that only Jobs with INACTIVE status can be updated

'''

import os
from streamsets.sdk import ControlHub

# Control Hub creds
cred_id = '' 
cred_token = ''


# List of Job names to update labels for
job_names = ['Job 1', 'Job 2']

# Lists of labels to add and remove
labels_to_add = ['new_label_1', 'new_label_2']
labels_to_remove = ['old_label_1', 'old_label_2']

# Method that returns a Job for a Job name
def get_job_by_name(sch, job_name):
    for job in sch.jobs:
        if job.job_name == job_name:
            return job
    print("Error: No Job found with the name " + job_name)
    return None

# Connect to Control Hub
print('---')
print('Connecting to Control Hub')
print('---')
sch = ControlHub(
    credential_id=cred_id,
    token=cred_token)

# Get the Jobs to set labels for
jobs_to_set_labels_for = []
for job_name in job_names:
    job = get_job_by_name(sch, job_name)
    if job is not None:
        jobs_to_set_labels_for.append(job)

# Update labels for Jobs
for job in jobs_to_set_labels_for:

    # Get the Job status
    job.refresh()
    job_status = job.status.status

    # Make sure Job has INACTIVE status
    if job_status == 'INACTIVE':

        print('Updating labels for Job: \'' + job.job_name + '\'')

        labels = job.data_collector_labels

        # Remove old labels
        for label in labels_to_remove:
            if label in labels:
                labels.remove(label)

        # Add new labels
        for label in labels_to_add:
            if label not in labels:
                labels.append(label)

        # Push the updates back to Control Hub
        sch.update_job(job)
    else:
        print('Error: Job \'' + job.job_name + '\' does not have the status of \'INACTIVE\', so its labels can\'t be updated')   

print('Done')
