#!/usr/bin/env python

'''
This script writes a rolling log file that contains CPU usage and JVM heap memory metrics
for a given Data Collector registered with StreamSets Platform, with a user definable refresh
interval.

Prerequisites:

 - Python 3.6+

 -  StreamSets Platform SDK for Python v5 or v6
    See: https://docs.streamsets.com/platform-sdk/latest/learn/installation.html

 - Control Hub API Credentials

 - Set the following variable in the script:

        # Control Hub URL
        sch_url = 'https://na01.hub.streamsets.com'

        # Set to True if WebSocket Communication is enabled
        # Set to False if Direct REST APIs are used
        websockets_enabled = True

        # A pre-existing directory to write the Data Collector metrics to
        output_dir = '/path/to/sdc-metrics'

        # How frequently to capture metrics
        metrics_capture_interval_seconds = 5 * 60 # 5 minutes

 - To avoid including API credentials in the script, export these environment variables
   prior to running the script:

          export CRED_ID="e9745d07...."
          export CRED_TOKEN="eyd..."

 - Run the script with an argument that is the URL of the Data Collector to monitor, like this:

            $ python get-sdc-metrics.py <SDC_URL>

     for example:

            $ python  get-sdc-metrics.py http://sequoia.onefoursix.com:11111

 - To run the script as a background process, set the variable print_metrics_to_console in the
   script to False and then launch the script using a command like this:

       $ nohup python get-sdc-metrics.py http://sequoia.onefoursix.com:11111 > /dev/null 2>&1 &

- Sample console output looks like this:

$ python get-sdc-metrics.py http://sequoia.onefoursix.com:11111

Getting resource metrics for Data Collector at http://sequoia.onefoursix.com:11111

{"sdc_url": "http://sequoia.onefoursix.com:11111", "timestamp": "2023-09-01 11:32:49", 
"heap_memory_used": 935726360, "heap_memory_max": 4216455168, "heap_memory_percentage": 22, 
"cpu_load_percentage": 5}

{"sdc_url": "http://sequoia.onefoursix.com:11111", "timestamp": "2023-09-01 11:33:04", 
"heap_memory_used": 468823488, "heap_memory_max": 4216455168, "heap_memory_percentage": 11, 
"cpu_load_percentage": 3}

{"sdc_url": "http://sequoia.onefoursix.com:11111", "timestamp": "2023-09-01 11:33:19", 
"heap_memory_used": 651639144, "heap_memory_max": 4216455168, "heap_memory_percentage": 15, 
"cpu_load_percentage": 3}

...


'''

# Imports
import os, sys, json, time, logging
from datetime import datetime
from streamsets.sdk import ControlHub
from logging.handlers import RotatingFileHandler

# Get Control Hub API credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')

# Control Hub URL
sch_url = 'https://na01.hub.streamsets.com'

# Set to True if WebSocket Communication is enabled
# Set to False if Direct REST APIs are used
websockets_enabled = True

# The directory and name for the rolling log file
output_dir = '/Users/mark/data/sdc-metrics'
log_file_name = 'sdc-metrics.log'

# How often to capture SDC metrics
metrics_capture_interval_seconds = 5 * 60 # 5 minutes

# Whether or not to print metrics to the console
print_metrics_to_console = True

# Rolling Logfile config
log_file = output_dir + '/' + log_file_name
max_bytes_pre_log_file = 100 * 1024 * 1024  # 100MB
number_of_rolling_logfiles = 5

# Method to create a rolling log file
def create_rotating_log():
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_file, maxBytes=max_bytes_pre_log_file, backupCount=number_of_rolling_logfiles)
    logger.addHandler(handler)
    return logger

# Validate command line args
if len(sys.argv) != 2:
    print('Incorrect number of arguments')
    print('Usage: python get-sdc-metrics.py <SDC_URL>')
    sys.exit(-1)

# Confirm the logging directory exists]
if not os.path.isdir(output_dir):
    print('Error: the directory \'' + output_dir + '\' does not exist')
    print('Please create that directory in advance')
    sys.exit(-1)

# Get the SDC URL from the command line
sdc_url = 'http://sequoia.onefoursix.com:11111' #sys.argv[1]

# Create the log file
logger = create_rotating_log()

# Connect to Control Hub
sch = None
try:
    sch = ControlHub(credential_id=cred_id, token=cred_token, use_websocket_tunneling=websockets_enabled)
except Exception as e:
    print('Error: Could not connect to Control Hub.')
    print('Check your API credentials and the Control Hub URL')
    print('Exception: ' + str(e))
    sys.exit(-1)

# Connect to the Data Collector
sdc = None
try:
    sdc =   sch.data_collectors.get(engine_url=sdc_url)._instance
except Exception as e:
    print('Error: Could not connect to Data Collector')
    print('Error; ' + str(e))
    sys.exit(-1)

print('Getting resource metrics for Data Collector at ' + sdc_url)

# Get Data Collector metrics in an endless loop until this script is stopped
while (True):
    try:
        jmx_metrics = sdc.get_jmx_metrics()
        heap_metrics = jmx_metrics.get('java.lang:type=Memory')['HeapMemoryUsage']
        cpu_metrics = jmx_metrics.get('java.lang:type=OperatingSystem')
        metrics = {}
        metrics['sdc_url'] = sdc_url
        metrics['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        metrics['heap_memory_used'] = heap_metrics['used']
        metrics['heap_memory_max'] = heap_metrics['max']
        metrics['heap_memory_percentage'] = int((heap_metrics['used'] / heap_metrics['max']) * 100)
        metrics['cpu_load_percentage'] = int(cpu_metrics['SystemCpuLoad'] * 100)

        # Convert the metrics to JSON
        data = json.dumps(metrics)

        # Print messages to the console if needed
        if print_metrics_to_console:
            print(data)

        # Write metrics to the rolling logfile
        logger.info(data)

    except Exception as e:
        print('Exception occurred while reading SDC metrics: ' + str(e))

    # Sleep
    time.sleep(metrics_capture_interval_seconds)
