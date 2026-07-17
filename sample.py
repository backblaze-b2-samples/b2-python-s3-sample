#!/usr/bin/env python3

# -*-coding:utf-8 -*-

""" A python script for working with Backblaze B2 """

""" More Instructions here:  https://www.backblaze.com/docs/cloud-storage-python-developer-quick-start-guide """
""" Video Code Walkthroughs - A playlist for related videos is here:   
        https://www.youtube.com/c/backblaze/playlists  """
""" This source code on Github here:  
    https://github.com/backblaze-b2-samples/b2-python-s3-sample/ """
""" Sample data in *PUBLIC* bucket is configured through B2_PUBLIC_URL_BASE. """

import boto3  # REQUIRED! - Details here: https://pypi.org/project/boto3/
from botocore.exceptions import ClientError
from botocore.config import Config
from dataclasses import dataclass
from dotenv import load_dotenv  # Project Must install Python Package:  python-dotenv
import os
import re
import sys

B2_USER_AGENT_EXTRA = 'b2-python-s3-sample (backblaze-b2-samples)'
B2_REGION_PATTERN = re.compile(r'^[a-z]{2,}(?:-[a-z]+)+-\d{3}$')
B2_DELETE_BATCH_SIZE = 1000
B2_MAX_LIST_RESULTS = 1000

DEFAULT_PUBLIC_BUCKET_NAME = 'developer-b2-quick-start'  # Bucket with Sample Data **PUBLIC**
DEFAULT_PRIVATE_BUCKET_NAME = 'developer-b2-quick-start-private'  # Bucket with Sample Data **PRIVATE**

file1 = "beach.jpg"     # Sample Data - files in public Bucket with Sample Data
file1_pri = "beach3.jpg"
file2 = "coconuts.jpg"
file3 = "sunset.jpg"
LOCAL_DIR = '.'  # <-- Current directory by default, you can change this to any directory that exists

WEEK_IN_SECONDS = 604800


@dataclass(frozen=True)
class B2ConfigValues:
    endpoint: str
    key_id: str
    application_key: str
    public_bucket_name: str
    private_bucket_name: str
    private_key_id: str
    private_application_key: str
    write_endpoint: str
    write_key_id: str
    write_application_key: str
    write_bucket_name: str
    transient_bucket_name: str | None
    public_url_base: str


def validate_b2_region(region):
    if not B2_REGION_PATTERN.fullmatch(region):
        raise RuntimeError(f'Invalid B2_REGION: {region!r}')
    return region


def get_b2_endpoint(region):
    return f'https://s3.{validate_b2_region(region)}.backblazeb2.com'


def get_env_value(name, legacy_names=(), required=True, default=None):
    value = os.getenv(name)
    if value:
        return value
    for legacy_name in legacy_names:
        value = os.getenv(legacy_name)
        if value:
            return value
    if default is not None:
        return default
    if required:
        expected_names = ' or '.join([name, *legacy_names])
        raise RuntimeError(f'Missing configuration. Set {expected_names} in the environment or .env file.')
    return None


def get_endpoint_config(region_name='B2_REGION', legacy_endpoint_names=('ENDPOINT',), required=True):
    region = os.getenv(region_name)
    if region:
        return get_b2_endpoint(region)
    for legacy_name in legacy_endpoint_names:
        endpoint = os.getenv(legacy_name)
        if endpoint:
            return endpoint.rstrip('/')
    if required:
        expected_names = ' or '.join([region_name, *legacy_endpoint_names])
        raise RuntimeError(f'Missing configuration. Set {expected_names} in the environment or .env file.')
    return None


def get_public_url_base(endpoint, bucket_name):
    configured_base = os.getenv('B2_PUBLIC_URL_BASE')
    if configured_base:
        return configured_base.rstrip('/')
    return f'{endpoint.rstrip("/")}/{bucket_name}'


def load_b2_config():
    endpoint = get_endpoint_config()
    key_id = get_env_value('B2_APPLICATION_KEY_ID', ('KEY_ID_RO',))
    application_key = get_env_value('B2_APPLICATION_KEY', ('APPLICATION_KEY_RO',))
    public_bucket_name = get_env_value(
        'B2_BUCKET_NAME',
        required=False,
        default=DEFAULT_PUBLIC_BUCKET_NAME,
    )
    has_private_identity = any(os.getenv(name) for name in (
        'B2_PRIVATE_APPLICATION_KEY_ID',
        'B2_PRIVATE_APPLICATION_KEY',
        'KEY_ID_PRIVATE_RO',
        'APPLICATION_KEY_PRIVATE_RO',
    ))
    private_bucket_name = get_env_value(
        'B2_PRIVATE_BUCKET_NAME',
        required=False,
        default=DEFAULT_PRIVATE_BUCKET_NAME if has_private_identity else public_bucket_name,
    )
    private_key_id = get_env_value(
        'B2_PRIVATE_APPLICATION_KEY_ID',
        ('KEY_ID_PRIVATE_RO',),
        required=False,
        default=key_id,
    )
    private_application_key = get_env_value(
        'B2_PRIVATE_APPLICATION_KEY',
        ('APPLICATION_KEY_PRIVATE_RO',),
        required=False,
        default=application_key,
    )
    write_endpoint = get_endpoint_config(
        legacy_endpoint_names=('ENDPOINT_URL_YOUR_BUCKET', 'ENDPOINT'),
        required=False,
    ) or endpoint
    write_key_id = get_env_value(
        'B2_WRITE_APPLICATION_KEY_ID',
        ('KEY_ID_YOUR_ACCOUNT',),
        required=False,
        default=key_id,
    )
    write_application_key = get_env_value(
        'B2_WRITE_APPLICATION_KEY',
        ('APPLICATION_KEY_YOUR_ACCOUNT',),
        required=False,
        default=application_key,
    )
    write_bucket_name = get_env_value(
        'B2_WRITE_BUCKET_NAME',
        required=False,
        default=public_bucket_name,
    )
    transient_bucket_name = get_env_value('B2_TRANSIENT_BUCKET_NAME', required=False)

    return B2ConfigValues(
        endpoint=endpoint,
        key_id=key_id,
        application_key=application_key,
        public_bucket_name=public_bucket_name,
        private_bucket_name=private_bucket_name,
        private_key_id=private_key_id,
        private_application_key=private_application_key,
        write_endpoint=write_endpoint,
        write_key_id=write_key_id,
        write_application_key=write_application_key,
        write_bucket_name=write_bucket_name,
        transient_bucket_name=transient_bucket_name,
        public_url_base=get_public_url_base(endpoint, public_bucket_name),
    )


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
def create_bucket(name, b2, private=False):
    try:
        acl = 'private' if private else 'public-read'
        b2.create_bucket(Bucket=name, ACL=acl)
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


# Delete the specified objects from B2 - all versions
def delete_files_all_versions(bucket, keys, client):
    paginator = client.get_paginator('list_object_versions')
    for key in keys:
        batch = []
        deleted_count = 0
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=key)
        for response in response_iterator:
            versions = response.get('Versions', [])
            versions.extend(response.get('DeleteMarkers', []))
            for version in versions:
                batch.append({'Key': key, 'VersionId': version['VersionId']})
                if len(batch) == B2_DELETE_BATCH_SIZE:
                    deleted_count += delete_object_version_batch(bucket, key, batch, client)
                    batch = []
        if batch:
            deleted_count += delete_object_version_batch(bucket, key, batch, client)
        print(f'Deleted {deleted_count} versions/delete markers from {bucket}/{key}')


def delete_object_version_batch(bucket, key, objects_to_delete, client):
    print(f'Deleting {len(objects_to_delete)} versions/delete markers from {bucket}/{key}')
    try:
        client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
    except ClientError as ce:
        print('error', ce)
        raise
    return len(objects_to_delete)

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
                             aws_secret_access_key=applicationKey, # Backblaze applicationKey
                             config=Config(
                                 signature_version='s3v4',
                                 user_agent_extra=B2_USER_AGENT_EXTRA,
                                 connect_timeout=10,
                                 read_timeout=30,
                                 retries={'max_attempts': 3, 'mode': 'standard'},
                             ))
    return b2_client


# Return a boto3 resource object for B2 service
def get_b2_resource(endpoint, key_id, application_key):
    b2 = boto3.resource(service_name='s3',
                        endpoint_url=endpoint,                # Backblaze endpoint
                        aws_access_key_id=key_id,              # Backblaze keyID
                        aws_secret_access_key=application_key, # Backblaze applicationKey
                        config=Config(
                            signature_version='s3v4',
                            user_agent_extra=B2_USER_AGENT_EXTRA,
                            connect_timeout=10,
                            read_timeout=30,
                            retries={'max_attempts': 3, 'mode': 'standard'},
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

# Iterate over the keys of the objects in the specified bucket
def iter_object_keys(bucket, b2, max_results=B2_MAX_LIST_RESULTS):
    try:
        response = b2.Bucket(bucket).objects.all()

        for index, object in enumerate(response, start=1):
            if max_results is not None and index > max_results:
                print(f'\nLISTING TRUNCATED after {max_results} objects')
                break
            yield object.key

    except ClientError as ce:
        print('error', ce)


# List the keys of the objects in the specified bucket
def list_object_keys(bucket, b2, max_results=B2_MAX_LIST_RESULTS):
    return list(iter_object_keys(bucket, b2, max_results=max_results))


def get_object_browsable_url(public_url_base, key):
    return "%s/%s" % (public_url_base.rstrip('/'), key)


# Iterate browsable URLs of the objects in the specified bucket - Useful for *PUBLIC* buckets
def iter_objects_browsable_url(bucket, public_url_base, b2, max_results=B2_MAX_LIST_RESULTS):
    for key in iter_object_keys(bucket, b2, max_results=max_results):
        yield get_object_browsable_url(public_url_base, key)


# List browsable URLs of the objects in the specified bucket - Useful for *PUBLIC* buckets
def list_objects_browsable_url(bucket, public_url_base, b2, max_results=B2_MAX_LIST_RESULTS):
    return list(iter_objects_browsable_url(bucket, public_url_base, b2, max_results=max_results))


def print_items(items):
    count = 0
    for item in items:
        print(item)
        count += 1
    return count


# Upload specified file into the specified bucket
def upload_file(bucket, directory, file, b2, b2path=None):
    file_path = directory + '/' + file
    remote_path = b2path
    if remote_path is None:
        remote_path = file

    response = b2.Bucket(bucket).upload_file(file_path, remote_path)

    return response


"""
Python main() 

Basic execution setup
Then conditional blocks executing based on command-line arguments passed as input.
"""
def main():
    args = sys.argv[1:]  # retrieve command-line arguments passed to the script

    load_dotenv()   # load environment variables from file .env

    config = load_b2_config()

    # Call function to return reference to B2 service
    b2 = get_b2_resource(config.endpoint, config.key_id, config.application_key)

    # Call function to return reference to B2 service
    b2_client = get_b2_client(config.endpoint, config.key_id, config.application_key)

    # Call function to return reference to B2 service using the private demo keys
    b2_private = get_b2_resource(
        config.endpoint,
        config.private_key_id,
        config.private_application_key,
    )

    # BEGIN if elif BLOCKS FOR EACH PARAM
    # 01 - list_objects
    if len(args) == 0 or (len(args) == 1 and args[0] == '01'):
        # Call function to return list of object 'keys'
        object_count = print_items(iter_object_keys(config.public_bucket_name, b2))

        print('\nBUCKET ', config.public_bucket_name, ' LISTED ', object_count, ' OBJECTS')

    # 02 - List Objects formatted as browsable url
    # IF *PUBLIC* BUCKET, PRINT OUTPUTS BROWSABLE URL FOR EACH FILE IN THE BUCKET
    elif len(args) == 1 and ( args[0] == '02' or args[0] == '02PUB' ):
        # Call function to return list of object 'keys' formatted into friendly urls
        url_count = print_items(iter_objects_browsable_url(config.public_bucket_name, config.public_url_base, b2))

        print('\nBUCKET ', config.public_bucket_name, ' LISTED ', url_count, ' OBJECTS')


    # IF *PRIVATE* BUCKET, PRINT OUTPUTS URL THAT WHEN USED IN BROWSER RETURNS ERROR: UnauthorizedAccess
    elif len(args) == 1 and args[0] == '02PRI':
        # Call function to return list of object 'keys' concatenated into friendly urls
        url_count = print_items(iter_objects_browsable_url(config.private_bucket_name, config.public_url_base, b2_private))

        print('\nBUCKET ', config.private_bucket_name, ' LISTED ', url_count, ' FILES')

    # 04 - LIST BUCKETS
    elif len(args) == 1 and args[0] == '04':

        list_buckets( b2_client, raw_object=True )


    # 05 - PRESIGNED URLS
    elif len(args) == 1 and args[0] == '05':
        my_bucket = b2_private.Bucket(config.private_bucket_name)
        print('my_bucket: ', my_bucket)
        for my_bucket_object in my_bucket.objects.all():
            file_url = get_object_presigned_url(
                config.private_bucket_name,
                my_bucket_object.key,
                3000,
                b2_private,
            )
            print (file_url)


    # 06 - DOWNLOAD FILE
    elif len(args) == 1 and args[0] == '06':

        download_file(bucket = config.private_bucket_name,
                      directory = LOCAL_DIR,
                      local_name = file1_pri,
                      key_name = file1_pri,
                      b2 = b2_private)


    # WRITE OPERATIONS - THIS BLOCK CREATES B2 OBJECTS THAT SUPPORT WRITE OPERATIONS FOR BUCKETS IN YOUR ACCOUNT
    if len(args) == 1 and args[0] >= '20':
        # Call function to return reference to B2 service
        b2_write = get_b2_resource(
            config.write_endpoint,
            config.write_key_id,
            config.write_application_key,
        )

        write_client = get_b2_client(
            config.write_endpoint,
            config.write_key_id,
            config.write_application_key,
        )

        # 20 - CREATE BUCKET - *SUCCESS*
        if len(args) == 1 and args[0] == '20':

            print('BEFORE CREATE NEW BUCKET NAMED:  ', config.write_bucket_name)
            list_buckets( write_client )

            b2 = b2_write

            #  How To Here:  https://help.backblaze.com/hc/en-us/articles/360047629793-How-to-use-the-AWS-SDK-for-Python-with-B2-
            response = b2.create_bucket( Bucket=config.write_bucket_name )

            print('RESPONSE:  ', response)

            print('\nAFTER CREATE BUCKET')
            list_buckets( write_client )

        # 21 - UPLOAD FILE
        elif len(args) == 1 and args[0] == '21':

            b2 = b2_write

            response = upload_file(config.write_bucket_name, LOCAL_DIR, file1_pri, b2)

            print('RESPONSE:  ', response)

            print(f'UPLOADED {file1_pri} to {config.write_bucket_name}')


        # 22 - Copy File Between Buckets
        elif len(args) == 1 and args[0] == '22':

            b2 = b2_write
            if not config.transient_bucket_name:
                raise RuntimeError('B2_TRANSIENT_BUCKET_NAME must be set for option 22')

            try:
                b2.create_bucket( Bucket=config.transient_bucket_name )
            except ClientError as e:
                if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                    print(f'Bucket {config.transient_bucket_name} already exists.\nTo run this operation,'
                          f' you must first remove the bucket contents, then the bucket itself.')
                    return
                raise

            client = write_client

            list_buckets( client )

            print('\nBEFORE CONTENTS BUCKET ', config.transient_bucket_name)
            copy_file(
                config.write_bucket_name,
                config.transient_bucket_name,
                file1_pri,
                file1_pri,
                b2,
            )

            print('\nAFTER CONTENTS BUCKET ', config.transient_bucket_name)
            my_bucket = b2.Bucket(config.transient_bucket_name)
            for my_bucket_object in my_bucket.objects.all():
                print(my_bucket_object.key)

        # 30 - Delete File
        elif len(args) == 1 and args[0] == '30':
            print('BEFORE - Bucket Contents ')
            b2 = b2_write
            my_bucket = b2.Bucket(config.write_bucket_name)
            for my_bucket_object in my_bucket.objects.all():
                print(my_bucket_object.key)

            print('File Name to Delete:  ', file1_pri)

            delete_files(config.write_bucket_name, [file1_pri], b2)

            print('\nAFTER - Bucket Contents ')
            my_bucket = b2.Bucket(config.write_bucket_name)
            for my_bucket_object in my_bucket.objects.all():
                print(my_bucket_object.key)

        # 31 - Delete File all versions
        elif len(args) == 1 and args[0] == '31':
            print('BEFORE - Bucket Contents ')
            b2 = b2_write
            my_bucket = b2.Bucket(config.write_bucket_name)
            for my_bucket_object in my_bucket.object_versions.all():
                print(f'KEY: {my_bucket_object.object_key}, ID: {my_bucket_object.id}')

            print('File Name to Delete (all versions):  ', file1_pri)

            client = write_client

            delete_files_all_versions(config.write_bucket_name,
                                      [file1_pri],
                                      client )

            print('\nAFTER - Bucket Contents ')
            my_bucket = b2.Bucket(config.write_bucket_name)
            for my_bucket_object in my_bucket.object_versions.all():
                print(f'KEY: {my_bucket_object.object_key}, ID: {my_bucket_object.id}')


        # Cannot delete non-empty bucket - error An error occurred (BucketNotEmpty) when calling the DeleteBucket operation:
        # 32 - Delete Bucket
        elif len(args) == 1 and args[0] == '32':

            client = write_client

            print('BEFORE - Buckets ')
            list_buckets( client )

            print('Bucket Name to Delete:  ', config.write_bucket_name)

            b2 = b2_write

            delete_bucket(config.write_bucket_name, b2)

            print('\nAFTER - Buckets ')
            list_buckets( client )


# Optional (not strictly required)
if __name__ == '__main__':
    main()
