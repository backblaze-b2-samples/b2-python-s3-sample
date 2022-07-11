#!/usr/bin/env python3

# -*-coding:utf-8 -*-

""" A python script for working with Backblaze B2 """

""" More Instructions here:  http://www.backblaze.com/b2/docs/s3/python.html """
""" Video Code Walkthroughs - A playlist for related videos is here:   
        https://www.youtube.com/c/backblaze/playlists  """
""" This source code on Github here:  
    https://github.com/backblaze-b2-samples/b2-python-s3-sample/ """
""" Sample data in *PUBLIC* bucket here:  
    https://s3.us-west-002.backblazeb2.com/developer-b2-quick-start/album/photos.html """

import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from botocore.config import Config
from dotenv import load_dotenv  # Project Must install Python Package:  python-dotenv
import os
import sys

PUBLIC_BUCKET_NAME = 'developer-b2-quick-start'  # Bucket with Sample Data **PUBLIC**
PRIVATE_BUCKET_NAME = 'developer-b2-quick-start-private'   # Bucket with Sample Data **PRIVATE**

# You create next two buckets in your own account. Created by functions below
# MUST BE UNIQUE NAMES!  GLOBALLY UNIQUE ACROSS ALL ACCOUNTS IN ALL REGIONS!
NEW_BUCKET_NAME = '<ENTER YOUR BUCKET NAME HERE!>'
TRANSIENT_BUCKET_NAME = '<ENTER YOUR BUCKET NAME HERE!>'

file1 = "beach.jpg"     # Sample Data - files in public Bucket with Sample Data
file1_pri = "beach3.jpg"
file2 = "coconuts.jpg"
file3 = "sunset.jpg"
LOCAL_DIR = 'C:\\tmp'  # <-- Make sure this directory exists on your local machine; adjust name per your operating system

WEEK_IN_SECONDS = 604800

# Copy the specified existing object in a B2 bucket, creating a new copy in a second B2 bucket
def copy_file(source_bucket, destination_bucket, source_key, destination_key, b2):
    try:
        source = {
            'Bucket': source_bucket,
            'Key': source_key
        }
        b2.Bucket(destination_bucket).copy(source, destination_key)
    except ClientError as ce:
        print('error', ce)


# Create the specified bucket on B2
def create_bucket(name, b2, secure=False):
    try:
        b2.create_bucket(Bucket=name)
        if secure:
            prevent_public_access(name, b2)
    except ClientError as ce:
        print('error', ce)


# Delete the specified bucket from B2
def delete_bucket(bucket, b2):
    try:
        b2.Bucket(bucket).delete()
    except ClientError as ce:
        print('error', ce)


# Delete the specified objects from B2
def delete_files(bucket, keys, b2):
    objects = []
    for key in keys:
        objects.append({'Key': key})
    try:
        b2.Bucket(bucket).delete_objects(Delete={'Objects': objects})
    except ClientError as ce:
        print('error', ce)


# Delete the specified object from B2 - all versions
def delete_files_all_versions(bucket, keys, client):
    objects = []
    for key in keys:
        objects.append({'Key': key})
    try:
        # SOURCE re LOGIC FOLLOWING:  https://stackoverflow.com/questions/46819590/delete-all-versions-of-an-object-in-s3-using-python
        paginator = client.get_paginator('list_object_versions')
        response_iterator = paginator.paginate(Bucket=bucket)
        for response in response_iterator:
            versions = response.get('Versions', [])
            versions.extend(response.get('DeleteMarkers', []))
            for version_id in [x['VersionId'] for x in versions
                               if x['Key'] == key and x['VersionId'] != 'null']:
                print('Deleting {} version {}'.format(key, version_id))
                client.delete_object(Bucket=bucket, Key=key, VersionId=version_id)

    except ClientError as ce:
        print('error', ce)

# Download the specified object from B2 and write to local file system
def download_file(bucket, directory, local_name, key_name, b2):
    file_path = directory + '/' + local_name
    try:
        b2.Bucket(bucket).download_file(key_name, file_path)
    except ClientError as ce:
        print('error', ce)


# Return a boto3 client object for B2 service
def get_b2_client(endpoint, keyID, applicationKey):
        b2_client = boto3.client(service_name='s3',
                                 endpoint_url=endpoint,                # Backblaze endpoint
                                 aws_access_key_id=keyID,              # Backblaze keyID
                                 aws_secret_access_key=applicationKey) # Backblaze applicationKey
        return b2_client


# Return a boto3 resource object for B2 service
def get_b2_resource(endpoint, key_id, application_key):
    b2 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,                # Backblaze endpoint
                        aws_access_key_id=keyID,              # Backblaze keyID
                        aws_secret_access_key=applicationKey, # Backblaze applicationKey
                        config = Config(
                            signature_version='s3v4',
                    ))
    return b2


# Return presigned URL of the object in the specified bucket - Useful for *PRIVATE* buckets
def get_object_presigned_url(bucket, key, expiration_seconds, b2):
    try:
        response = b2.meta.client.generate_presigned_url(ClientMethod='get_object',
                                                         ExpiresIn=expiration_seconds,
                                                         Params={
                                                                    'Bucket': bucket,
                                                                    'Key': key
                                                                } )
        return response

    except ClientError as ce:
        print('error', ce)


# List the buckets in account in the specified region
def list_buckets(b2_client, raw_object=False):
    try:
        my_buckets_response = b2_client.list_buckets()

        print('\nBUCKETS')
        for bucket_object in my_buckets_response[ 'Buckets' ]:
            print(bucket_object[ 'Name' ])

        if raw_object:
            print('\nFULL RAW RESPONSE:')
            print(my_buckets_response)

    except ClientError as ce:
        print('error', ce)

# List the keys of the objects in the specified bucket
def list_object_keys(bucket, b2):
    try:
        response = b2.Bucket(bucket).objects.all()

        return_list = []               # create empty list
        for object in response:        # iterate over response
            return_list.append(object.key) # for each item in response append object.key to list
        return return_list             # return list of keys from response

    except ClientError as ce:
        print('error', ce)


# List browsable URLs of the objects in the specified bucket - Useful for *PUBLIC* buckets
def list_objects_browsable_url(bucket, endpoint, b2):
    try:
        bucket_object_keys = list_object_keys(bucket, b2)

        return_list = []                # create empty list
        for key in bucket_object_keys:  # iterate bucket_objects
            url = "%s/%s/%s" % (endpoint, bucket, key) # format and concatenate strings as valid url
            return_list.append(url)     # for each item in bucket_objects append value of 'url' to list
        return return_list              # return list of keys from response

    except ClientError as ce:
        print('error', ce)


# Upload specified file into the specified bucket
def upload_file(bucket, directory, file, b2, b2path=None):
    file_path = directory + '/' + file
    remote_path = b2path
    if remote_path is None:
        remote_path = file
    try:
        response = b2.Bucket(bucket).upload_file(file_path, remote_path)
    except ClientError as ce:
        print('error', ce)

    return response


"""
Python main() 

Basic execution setup
Then conditional blocks executing based on command-line arguments passed as input.
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

    # Call function to return reference to B2 service
    b2_client = get_b2_client(endpoint, key_id_ro, application_key_ro)

    # get environment variables from file .env
    key_id_private_ro = os.getenv("KEY_ID_PRIVATE_RO")  # Backblaze keyID
    application_key_private_ro = os.getenv("APPLICATION_KEY_PRIVATE_RO") # Backblaze applicationKey

    # Call function to return reference to B2 service using a second set of keys
    b2_private = get_b2_resource(endpoint, key_id_private_ro, application_key_private_ro)

    # WRITE OPERATIONS - THIS BLOCK CREATES B2 OBJECTS THAT SUPPORT WRITE OPERATIONS FOR BUCKETS IN YOUR ACCOUNT
    if len(args) == 1 and args[0] >= '20':
        endpoint_rw = os.getenv("ENDPOINT_URL_YOUR_BUCKET")  # getting environment variables from file .env
        key_id_rw = os.getenv("KEY_ID_YOUR_ACCOUNT")  # getting environment variables from file .env
        application_key_rw = os.getenv("APPLICATION_KEY_YOUR_ACCOUNT")  # getting environment variables from file .env
        # Call function to return reference to B2 service
        b2_rw = get_b2_resource(endpoint_rw, key_id_rw, application_key_rw)

        # Call function to return reference to B2 service
        b2_client_rw = get_b2_client(endpoint_rw, key_id_rw, application_key_rw)

    # BEGIN if elif BLOCKS FOR EACH PARAM
    # 01 - list_objects
    if len(args) == 0 or (len(args) == 1 and args[0] == '01'):
        # Call function to return list of object 'keys'
        bucket_object_keys = list_object_keys(PUBLIC_BUCKET_NAME, b2)
        for key in bucket_object_keys:
            print(key)

        print('\nBUCKET ', PUBLIC_BUCKET_NAME, ' CONTAINS ', len(bucket_object_keys), ' OBJECTS')

    # 02 - List Objects formatted as browsable url
    # IF *PUBLIC* BUCKET, PRINT OUTPUTS BROWSABLE URL FOR EACH FILE IN THE BUCKET
    elif len(args) == 1 and ( args[0] == '02' or args[0] == '02PUB' ):
        # Call function to return list of object 'keys' formatted into friendly urls
        browsable_urls = list_objects_browsable_url(PUBLIC_BUCKET_NAME, endpoint, b2)
        for url in browsable_urls:
            print(url)

        print('\nBUCKET ', PUBLIC_BUCKET_NAME, ' CONTAINS ', len(browsable_urls), ' OBJECTS')


    # IF *PRIVATE* BUCKET, PRINT OUTPUTS URL THAT WHEN USED IN BROWSER RETURNS ERROR: UnauthorizedAccess
    elif len(args) == 1 and args[0] == '02PRI':
        # Call function to return list of object 'keys' concatenated into friendly urls
        browsable_urls = list_objects_browsable_url(PRIVATE_BUCKET_NAME, endpoint, b2_private)
        for key in browsable_urls:
            print(key)

        print('\nBUCKET ', PRIVATE_BUCKET_NAME, ' CONTAINS ', len(browsable_urls), ' FILES')

    # 04 - LIST BUCKETS
    elif len(args) == 1 and args[0] == '04':

        list_buckets( b2_client, raw_object=True )


    # 05 - PRESIGNED URLS
    elif len(args) == 1 and args[0] == '05':
        my_bucket = b2_private.Bucket(PRIVATE_BUCKET_NAME)
        print('my_bucket: ', my_bucket)
        for my_bucket_object in my_bucket.objects.all():
            file_url = get_object_presigned_url(PRIVATE_BUCKET_NAME, my_bucket_object.key, 3000, b2_private)
            print (file_url)


    # 06 - DOWNLOAD FILE
    elif len(args) == 1 and args[0] == '06':

        download_file(bucket = PRIVATE_BUCKET_NAME,
                      directory = LOCAL_DIR,
                      local_name = file1_pri,
                      key_name = file1_pri,
                      b2 = b2_private)


    # 20 - CREATE BUCKET - *SUCCESS*
    elif len(args) == 1 and args[0] == '20':

        print('BEFORE CREATE NEW BUCKET NAMED:  ',NEW_BUCKET_NAME )
        list_buckets( b2_client )

        b2 = b2_rw

        #  How To Here:  https://help.backblaze.com/hc/en-us/articles/360047629793-How-to-use-the-AWS-SDK-for-Python-with-B2-
        response = b2.create_bucket( Bucket=NEW_BUCKET_NAME )

        print('RESPONSE:  ', response)

        print('\nAFTER CREATE BUCKET')
        list_buckets( b2_client )

    # 21 - UPLOAD FILE
    elif len(args) == 1 and args[0] == '21':

        b2 = b2_rw

        response = upload_file(NEW_BUCKET_NAME, LOCAL_DIR, file1, b2)

        print('RESPONSE:  ', response)

        generate_friendly_url( NEW_BUCKET_NAME, endpoint, b2)


    # 22 - Copy File Between Buckets
    elif len(args) == 1 and args[0] == '22':

        b2 = b2_rw

        response = b2.create_bucket( Bucket=TRANSIENT_BUCKET_NAME )

        list_buckets( b2_client )

        print('\nBEFORE CONTENTS BUCKET ', TRANSIENT_BUCKET_NAME)
        copy_file(NEW_BUCKET_NAME, TRANSIENT_BUCKET_NAME, file1, file1, b2)

        print('\nAFTER CONTENTS BUCKET ', TRANSIENT_BUCKET_NAME)
        my_bucket = b2.Bucket(TRANSIENT_BUCKET_NAME)
        for my_bucket_object in my_bucket.objects.all():
            print(my_bucket_object.key)

    # 30 - Delete File
    elif len(args) == 1 and args[0] == '30':
        print('BEFORE - Bucket Contents ')
        my_bucket = b2.Bucket(NEW_BUCKET_NAME)
        for my_bucket_object in my_bucket.objects.all():
            print(my_bucket_object.key)

        print('File Name to Delete:  ', file1)

        b2 = b2_rw

        delete_files(NEW_BUCKET_NAME, [file1], b2)

        print('\nAFTER - Bucket Contents ')
        my_bucket = b2.Bucket(NEW_BUCKET_NAME)
        for my_bucket_object in my_bucket.objects.all():
            print(my_bucket_object.key)

    # 31 - Delete File all versions
    elif len(args) == 1 and args[0] == '31':
        print('BEFORE - Bucket Contents ')
        b2 = b2_rw
        my_bucket = b2.Bucket(NEW_BUCKET_NAME)
        for my_bucket_object in my_bucket.objects.all():
            print(my_bucket_object.key)

        print('File Name to Delete:  ', file1)

        b2_client = b2_client_rw

        delete_files_all_versions(NEW_BUCKET_NAME,
                                  [file1],
                                  b2_client )

        print('\nAFTER - Bucket Contents ')
        my_bucket = b2.Bucket(NEW_BUCKET_NAME)
        for my_bucket_object in my_bucket.objects.all():
            print(my_bucket_object.key)


    # Cannot delete non-empty bucket - error An error occurred (BucketNotEmpty) when calling the DeleteBucket operation:
    # 32 - Delete Bucket
    elif len(args) == 1 and args[0] == '32':

        print('BEFORE - Buckets ')
        list_buckets( b2_client )

        print('Bucket Name to Delete:  ', NEW_BUCKET_NAME)

        b2 = b2_rw

        delete_bucket(NEW_BUCKET_NAME, b2)

        print('\nAFTER - Buckets ')
        list_buckets( b2_client )


# Optional (not strictly required)
if __name__ == '__main__':
    main()
