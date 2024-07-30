#!/usr/bin/python3

"""
FILE: add_streamsets_user.py

DESCRIPTION: Adds a user to a StreamSets Platform Organization.
             If the user already exists in the Organization, no action is taken.
             (Users are identified by their email addresses).

ARGS:
    - user_email  # The user's email address

USAGE: $ python3 add_streamsets_user.py <user_email>

USAGE EXAMPLE: python3 add_streamsets_user.py mark.brooks@ibm.com


PREREQUISITES:

 - Python 3.9+

 - StreamSets Platform SDK for Python v6.0+
   See: https://docs.streamsets.com/platform-sdk/latest/welcome/installation.html

 - StreamSets Platform API Credentials for a user with Organization Administrator role

- Before running the script, export the environment variables CRED_ID and CRED_TOKEN
  with the StreamSets Platform API Credentials, like this:

    $ export CRED_ID="40af8..."
    $ export CRED_TOKEN="eyJ0..."

- Add any groups you want to add the new user to in the groups list variable below

"""
import os
from streamsets.sdk import ControlHub
import sys

# Groups to add the new user to. In this case we'll add the user
# to the "all" and "hol" (Hands On Lab) groups
groups = ['all', 'hol']


# Method to add a user to an Organization
def invite_user_to_org(control_hub, user_email, the_org_name):
    print('Inviting user with email \'{}\' to the Org \'{}\''.format(email, the_org_name))

    # Create a user_builder
    user_builder = sch.get_user_builder()

    # Build a user for the given email address
    user = user_builder.build(email_address=user_email)

    # Add the user to the desired groups
    for group in groups:
        try:
            print('Adding the user to the group \'{}\''.format(group))
            user.groups.append(sch.groups.get(display_name=group))
        except ValueError:
            print('Error: the group \'{}\' does not exist'.format(group))

    # Send an email invite for the user to set their password
    control_hub.invite_user(user)


# Get Control Hub Credentials from the environment
cred_id = os.getenv('CRED_ID')
cred_token = os.getenv('CRED_TOKEN')


def print_usage_and_exit():
    print('Usage: $ python3 add_streamsets_user.py <user_email>')
    print('Usage Example: python3 add_streamsets_user.py mark.brooks@ibm.com')
    sys.exit(1)


# Check the number of command line args
if len(sys.argv) != 2:
    print('Error: Wrong number of arguments')
    print_usage_and_exit()

email = sys.argv[1]

# Connect to Control Hub
sch = None
try:
    sch = ControlHub(
        credential_id=cred_id,
        token=cred_token)
except Exception as e:
    print('Error connecting to Control Hub; check your CRED_ID and CRED_TOKEN environment variables')
    print(str(e))
    sys.exit(1)

# Get the Organization's name
org_name = sch.organizations[0].name

# See if user already exists
try:
    if sch.users.get(email_address=email):
        print('User with email \'{}\' already exists in the Org \'{}\''.format(email, org_name))
        print('No action will be taken')

# If we get here, the user does not exist in the Org
except ValueError:

    # Add the user to the Organization
    invite_user_to_org(sch, email, org_name)

print('Done')
