#!/usr/bin/env python

'''
This script adds and removes engine labels at
both the engine level and the deployment level.

Setting labels at the engine level makes the labels
immediately available for running Jobs, though if an engine dies and is 
recreated (for example on k8s) labels set at the engine level will not persist

Setting labels at the deployment level makes the labels
available for running Jobs only after a deployment restart, or if engines 
are completely shutdown and restarted (for example, if you delete an 
engine Pod on K8s and let it get recreated). Updated labels set at the deployment
level will not take effect if one only performs a "restart engine" 
command from Control Hub.

Comment out either section if you only want to update labels and the engine
level or the deployment level

The script prints output to the console like this, grouping together
the deployments and their associated engine(s).

 % python update-engine-labels.py
---
Connecting to Control Hub
---
Updating labels for Deployment: rancher-1
Updating labels for Engine with URL: http://streamsets-deployment-4c58a2e2-3d47-4c92-a934-9808a6b97727p64mx:18630
---
Updating labels for Deployment: rancher-2
Updating labels for Engine with URL: http://streamsets-deployment-dec74a8f-961d-49b9-a8fd-f14030010feds92zn:18630
---
Done

'''

import os
from streamsets.sdk import ControlHub

# Control Hub creds
cred_id = '' 
cred_token = '' 

# List of deployment names to update engine labels for
deployment_names = ['rancher-1', 'rancher-2']

# Lists of labels to add and remove
labels_to_add = ['new_label_1', 'new_label_2']
labels_to_remove = ['old_label_1', 'old_label_2', ]

# Method that returns a deployment for a deployment name
def get_deployment_by_name(sch, deployment_name):
    for deployment in sch.deployments:
        if deployment.deployment_name == deployment_name:
            return deployment
    print("Error: No Deployment found with the name " + deployment_name)
    return None

# Connect to Control Hub
print('---')
print('Connecting to Control Hub')
print('---')
sch = ControlHub(
    credential_id=cred_id,
    token=cred_token)

# Get the deployments to set labels for
deployments_to_set_labels_for = {}
for deployment_name in deployment_names:
    deployment = get_deployment_by_name(sch, deployment_name)
    if deployment is not None:
        deployments_to_set_labels_for[deployment.deployment_id] = deployment


# Update labels for deployments
for deployment_id in deployments_to_set_labels_for.keys():

    deployment = deployments_to_set_labels_for[deployment_id]
    labels = deployment.engine_configuration.engine_labels

    print('Updating labels for Deployment: ' + deployment.deployment_name)

    # Remove old labels
    for label in labels_to_remove:
        if label in labels:
            labels.remove(label)

    # Add new labels
    for label in labels_to_add:
        if label not in labels:
            labels.append(label)

    # Push the updates back to Control Hub
    sch.update_deployment(deployment)

    # Get the engines that belong to the deployment
    for engine in sch.engines:
        if engine.deployment_id == deployment_id:
            print('Updating labels for Engine with URL: ' + engine.engine_url)

            # Remove old labels
            for label in labels_to_remove:
                if label in engine.labels:
                    engine.labels.remove(label)

            # Add new labels
            for label in labels_to_add:
                if label not in engine.labels:
                    engine.labels.append(label)

            # Push the updates back to Control Hub
            sch.update_engine_labels(engine)
    print('---')

print('Done')
