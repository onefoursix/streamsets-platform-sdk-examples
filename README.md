# streamsets-dataops-sdk-examples

This project includes examples of using [StreamSets DataOps Platform SDK for Python](https://docs.streamsets.com/platform-sdk/latest/index.html)

###  Control Hub API Credentials



The examples assume you have set and exported your Control Hub <code>CRED_ID</code> and <code>CRED_TOKEN</code> as environment variables before you run the Python scripts. For example:

    export CRED_ID=be87...
    export CRED_TOKEN=eyJ0eX...

This allows the Python scripts to avoid having <code>CRED_ID</code> and <code>CRED_TOKEN</code> values within the scripts.

## Examples

<hr/>

### - [dataops-start-job.py](dataops-start-job.py)

This example starts a Job.

Set the JOB_ID within the script, and optionally, a set of Job Runtime Parameters, then execute the script. You should see output like this:
````
$ python3 dataops-start-job.py
2023-03-13 21:19:20 Connecting to Control Hub
2023-03-13 21:19:21 Found Job with name 'Job for Pipeline_1'
2023-03-13 21:19:21 Job status is 'INACTIVE'
2023-03-13 21:19:21 Setting Job parameters...
2023-03-13 21:19:21 Starting Job...
2023-03-13 21:19:31 Job status is ACTIVE
2023-03-13 21:19:31 Done
````
<hr/>

### - [dataops-restart-job.py](dataops-restart-job.py)

This example restarts a Job.

Same as the previous example, but this time the script will stop the Job if it is already running, wait for the Job to transition to an INACTIVE state,. and then start it again.
````
$ python3 dataops-restart-job.py
2023-03-13 21:35:03 Connecting to Control Hub
2023-03-13 21:35:05 Found Job with name 'Job for Pipeline_1'
2023-03-13 21:35:05 Job status is 'ACTIVE'
2023-03-13 21:35:05 Attempting to stop Job...
2023-03-13 21:35:18 Waiting for Job to become INACTIVE
2023-03-13 21:35:28 Job status is INACTIVE
2023-03-13 21:35:28 Setting Job parameters...
2023-03-13 21:35:28 Starting Job...
2023-03-13 21:35:38 Job status is ACTIVE
2023-03-13 21:35:38 Done
````
<hr/>

### - [dataops-stop-job.py](dataops-stop-job.py)

This example stops a Job.

Set the JOB_ID within the scriptthen execute the script. You should see output like this:
````
$ python3 dataops-stop-job.py
2023-03-13 21:40:29 Connecting to Control Hub
2023-03-13 21:40:31 Found Job with name 'Job for Pipeline_1'
2023-03-13 21:40:31 Job status is 'ACTIVE'
2023-03-13 21:40:31 Stopping Job...
2023-03-13 21:40:40 Job status is INACTIVE
2023-03-13 21:40:40 Done
````
<hr/>

### - [dataops-start-job-template-instances.py](dataops-start-job-template-instances.py)

This example creates and starts a set of Job Template Instances.

Set the Job Template ID as well as a set of Job Template Instance runtime parameter values within the script, then run the script.  The number of Job Template Instances that are launched depends on the size of the Job Template Instance runtime parameter array you set. 

For example, the script sets the runtime parameters like this:
 
<code>RUNTIME_PARAMETERS = [{'PARAM_1': 'aaa', 'PARAM_2': '111'}, {'PARAM_1': 'bbb', 'PARAM_2': '222'}]</code>

with two sets of parameters, which  results in two Job Template instances being created and started. You should see output like this.
````
$ python3 dataops-start-job-template-instances.py
2023-03-13 21:47:29 Connecting to Control Hub
2023-03-13 21:47:31 Found Job Template with name 'Job Template for Pipeline_1'
2023-03-13 21:47:31 Starting 2 Job Template Instances
2023-03-13 21:48:08 Done
````
<hr/>

### - [dataops-build-pipeline-and-job.py](dataops-build-pipeline-and-job.py)
This example programmatically builds a Pipeline and Job and publishes them to Control Hub.

The script builds a tricial "Dev Data Generator to Trash" pipeline but also includes in the comments the code for a more complex JDBC to Kafka pipeline.

With the script , set the variable <code>SDC_URL</code> to the URL of an authoring SDC. For example, in my environment I have set that vairalbe like this:

<code>SDC_URL = 'http://10.10.10.169:18992'</code>

The console output looks like this:

````
$ python3 dataops-build-pipeline-and-job.py
2023-03-13 21:54:08 Connecting to Control Hub
2023-03-13 21:54:12 Adding pipeline to Control Hub
2023-03-13 21:54:14 Adding Job to Control Hub
2023-03-13 21:54:14 Done
````
<hr/>

### - [dataops-get-job-metrics.py](dataops-get-job-metrics.py)

This example retrieved metrics for a given Job.

Set the JOB_ID in the script and excecute it. Example output is:

````
$ python3 dataops-get-job-metrics.py
2023-03-13 21:56:16 Connecting to Control Hub
2023-03-13 21:56:18 Found Job with name 'Job for Pipeline_1'
----------
Run Number: 4
Input Count: 298000
Output Count: 298000
Total Error Count: 0
----------
----------
Run Number: 3
Input Count: 933000
Output Count: 933000
Total Error Count: 0
----------
----------
Run Number: 2
Input Count: 266000
Output Count: 266000
Total Error Count: 0
----------
----------
Run Number: 1
Input Count: 368000
Output Count: 368000
Total Error Count: 0
----------
2023-03-13 21:56:18 Done
````
<hr/>

### - [dataops-get-sdc-metrics.py](dataops-get-sdc-metrics.py)

This example gets metrics for all Data Collectors as well as a list of all pipelines running per SDC.  

Execute the script and the output should look like this:

````
% python3 dataops-get-sdc-metrics.py
2023-03-13 22:01:56 Connecting to Control Hub
----------
SDC URL: http://mark-ss.internal.cloudapp.net:18630
CPU LOAD: 0.49
MEMORY (MB): 987.59
Running Pipelines Count: 4
Running Pipelines:
{'pipeline': 'Weather to ADLS', 'type': 'Job', 'status': 'DISCONNECTED', 'last_reported_time': 1678480415198, 'message': 'The pipeline was stopped because the node process was shutdown. '}
{'pipeline': 'Weather to  S3', 'type': 'Job', 'status': 'DISCONNECTED', 'last_reported_time': 1678480415198, 'message': 'The pipeline was stopped because the node process was shutdown. '}
{'pipeline': 'Get Weather Events', 'type': 'Job', 'status': 'DISCONNECTED', 'last_reported_time': 1678480415198, 'message': 'The pipeline was stopped because the node process was shutdown. '}
{'pipeline': 'Weather to Snowflake', 'type': 'Job', 'status': 'DISCONNECTED', 'last_reported_time': 1678480415198, 'message': 'The pipeline was stopped because the node process was shutdown. '}
----------
----------
SDC URL: http://10.10.10.169:18992
CPU LOAD: 0.62
MEMORY (MB): 2956.88
Running Pipelines Count: 2
Running Pipelines:
{'pipeline': 'Job Template for Pipeline_1 - aaa', 'type': 'Job', 'status': 'RUNNING', 'last_reported_time': 1678770084783, 'message': None}
{'pipeline': 'Job Template for Pipeline_1 - bbb', 'type': 'Job', 'status': 'RUNNING', 'last_reported_time': 1678770084783, 'message': None}
----------
----------
SDC URL: http://streamsetedge.test1.com:18630
CPU LOAD: 0.13
MEMORY (MB): 1014.38
Running Pipelines Count: 0
Running Pipelines:
----------
2023-03-13 22:01:58 Done
````
<hr/>


### - [dataops-promote-pipeline-version-for-job.py](dataops-promote-pipeline-version-for-job)

This example upgrades a Job to run a different version of a pipeline than a Job is currently running.  This is typically part of a CI/CD process where a new version of a pipeline is promoted to a higher environment.  If the Job is already running, the script will upgrade and then restart the Job.

Within the script, set the JOB_ID and the new verison of the pipeline to upgrade the JOb to.  Here is sample output:

````
$ python3 dataops-promote-pipeline-version-for-job.py
2023-03-13 22:08:56 Connecting to Control Hub
2023-03-13 22:08:58 Found Job with name 'Job for Pipeline_1'
2023-03-13 22:08:58 Job's pipeline name: Pipeline_1
2023-03-13 22:08:58 Job's current pipeline version: 4'
2023-03-13 22:08:58 Looking for pipeline version '6' of pipeline 'Pipeline_1'...
2023-03-13 22:08:58 Found version '6' of pipeline 'Pipeline_1'
2023-03-13 22:08:58 Upgrading Job to version '6' of pipeline 'Pipeline_1'
2023-03-13 22:08:59 Done
````

<hr/>

### - [dataops-backup.py](dataops-backup.py)

This example exports Fragments, Pipelines, Jobs, and Job Templates.

Set the variable <code>EXPORT_BASE_DIR</code> within the script and execute it.

As DRAFT versions of Fragment and Pipelines are not exportable, teh script will export th elatests commit (if one exists) for Fragments and Pipelines that have DRAFT versions.

````
~$ python3 dataops-backup.py

Exporting resources to /home/mark/streamsets-export

----------------------------------------
Exporting Fragments
----------------------------------------
Exporting fragment'Apply XSL to XML' version '5'

Exporting fragment'Audit' version '1'

Here is an example message for a DRAFT version of a Fragment for which the last committed version is exported instead:

Fragment 'S3-Origin' version '2-DRAFT' will not be exported because it is a DRAFT version.
--> Exporting Fragment 'S3-Origin version '1' instead.
...
````

And here is an example message for a version 1-DRAFT of a Fragment for which nothing is exported:

````
Fragment 'SDC Record to XML' version '1-DRAFT' has no committed versions and will not be exported.
````

When the script completes, you should see four child directories created within the export base directory, like this:

````
$ ls -l /home/mark/streamsets-export/
total 32
drwxrwxr-x 2 mark mark  4096 Mar 11 01:46 fragments
drwxrwxr-x 2 mark mark  4096 Mar 11 01:49 job-templates
drwxrwxr-x 2 mark mark  4096 Mar 11 01:49 jobs
drwxrwxr-x 2 mark mark 20480 Mar 11 01:48 pipelines
````

Within each child directory you should see individual zip archives for each resource that was exported.  For example:

````
$ ls -l /home/mark/streamsets-export/pipelines/
-rw-rw-r-- 1 mark mark  59977 Mar 11 01:46 'ADLS to Azure Synapse.zip'
-rw-rw-r-- 1 mark mark  54639 Mar 11 01:46 'ADLS to Kafka.zip'
-rw-rw-r-- 1 mark mark  59055 Mar 11 01:46 'ADLS to Snowflake.zip'
-rw-rw-r-- 1 mark mark  50172 Mar 11 01:46 'AWS Secret Test.zip'
-rw-rw-r-- 1 mark mark  55461 Mar 11 01:46 'Async HTTP Calls using Jython.zip'
-rw-rw-r-- 1 mark mark  51213 Mar 11 01:46 'Avro to Kafka.zip
…
````


<hr/>

