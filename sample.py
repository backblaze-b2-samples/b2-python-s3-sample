#!/usr/bin/env python3

# -*-coding:utf-8 -*-

""" A python script for working with Backblaze B2 """

""" More Instructions here:  http://www.backblaze.com/b2/docs/s3/python.html """
""" Video Code Walkthrough here: https://youtu.be/1nw0gYMhFJg """
""" This source code on Github here:  
    https://github.com/backblaze-b2-samples/b2-python-s3-sample/ """
""" Sample data in *PUBLIC* bucket here:  
    https://s3.us-west-002.backblazeb2.com/developer-b2-quick-start/album/photos.html """

import os
import sys

import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from dotenv import load_dotenv  # Project Must install Python Package:  python-dotenv

PUBLIC_BUCKET_NAME = 'developer-b2-quick-start'  # Bucket with Sample Data **PUBLIC**
file1 = "beach.jpg"     # Sample Data - files in public Bucket with Sample Data
file2 = "coconuts.jpg"
file3 = "sunset.jpg"

# Return a boto3 resource object for B2 service
def get_b2_resource(endpoint, key_id, application_key):
    b2 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,                 # Backblaze endpoint
                        aws_access_key_id=key_id,              # Backblaze keyID
                        aws_secret_access_key=application_key) # Backblaze applicationKey
    return b2


# List the objects in the specified bucket
def list_objects(bucket, b2):
    try:
        response = b2.Bucket(bucket).objects.all()

        return_list = []               # create empty list
        for object in response:        # iterate over response
            return_list.append(object) # for each item in response append object to list
        return return_list             # return list of objects from response

    except ClientError as ce:
        print('error', ce)


# List browsable URLs of the objects in the specified bucket - Useful for *PUBLIC* buckets
def list_objects_friendly_url(bucket, endpoint, b2):
    try:
        bucket_objects = list_objects(bucket, b2)

        return_list = []               # create empty list
        for object in bucket_objects:  # iterate bucket_objects
            url = "%s/%s/%s" % (endpoint, bucket, object.key)
            return_list.append(url)    # for each item in bucket_objects append value of 'url' to list
        return return_list             # return list of keys from response

    except ClientError as ce:
        print('error', ce)


"""
Python main() 

Execute with no command-line arguments, or '01' as a command-line argument, 
to print a list of object keys from the sample bucket.

Execute with command-line argument '02' to print a list of object URLs from 
the sample bucket.
"""
def main():
    args = sys.argv[1:]  # retrieve command-line arguments passed to the script

    load_dotenv()   # load environment variables from file .env

    # get environment variables from file .env
    endpoint = os.getenv("ENDPOINT")  # Backblaze endpoint
    key_id_ro = os.getenv("KEY_ID_RO")  # Backblaze keyID
    application_key_ro = os.getenv("APPLICATION_KEY_RO") # Backblaze applicationKey

    # Call function to return reference to B2 service
    b2 = get_b2_resource(endpoint, key_id_ro, application_key_ro)

    # 01 - list_objects
    if (len(args) == 1 and args[0] == '01')  or (len(args) == 0):
        # Call function to return list of object 'keys'
        bucket_object_keys = list_objects(PUBLIC_BUCKET_NAME, b2)
        for object in bucket_object_keys:
            print(object.key)

        print('\nBUCKET ', PUBLIC_BUCKET_NAME, ' CONTAINS ', len(bucket_object_keys), ' OBJECTS')

    # 02 - List Objects formatted as browsable url
    # IF *PUBLIC* BUCKET, PRINT OUTPUTS BROWSABLE URL FOR EACH FILE IN THE BUCKET
    elif len(args) == 1 and ( args[0] == '02' or args[0] == '02PUB' ):
        # Call function to return list of object 'keys' formatted into friendly urls
        friendly_urls = list_objects_friendly_url( PUBLIC_BUCKET_NAME, endpoint, b2)
        for url in friendly_urls:
            print(url)

        print('\nBUCKET ', PUBLIC_BUCKET_NAME, ' CONTAINS ', len(friendly_urls), ' OBJECTS')

# Optional (not strictly required)
if __name__ == '__main__':
    main()
