import os
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

import sample


class FakeObject:
    def __init__(self, key):
        self.key = key


class FakeObjects:
    def __init__(self, keys):
        self.keys = keys

    def all(self):
        return iter(FakeObject(key) for key in self.keys)


class FakeBucket:
    def __init__(self, keys):
        self.objects = FakeObjects(keys)


class FakeB2:
    def __init__(self, keys):
        self.keys = keys

    def Bucket(self, bucket):
        return FakeBucket(self.keys)


class FakePaginator:
    def paginate(self, **kwargs):
        versions = [{'VersionId': f'v{i}'} for i in range(1001)]
        return [{'Versions': versions, 'DeleteMarkers': []}]


class FakeClient:
    def __init__(self):
        self.delete_calls = []

    def get_paginator(self, name):
        return FakePaginator()

    def delete_objects(self, **kwargs):
        self.delete_calls.append(kwargs['Delete']['Objects'])


class B2SampleConfigTests(unittest.TestCase):
    def test_region_validation_rejects_host_injection(self):
        bad_regions = [
            'us-west-002.attacker.example/a',
            'us-west-002:443',
            'https://us-west-002',
            'us.west.002',
        ]
        for region in bad_regions:
            with self.subTest(region=region):
                with self.assertRaises(RuntimeError):
                    sample.get_b2_endpoint(region)

    def test_region_validation_accepts_backblaze_region_token(self):
        self.assertEqual(
            sample.get_b2_endpoint('us-west-002'),
            'https://s3.us-west-002.backblazeb2.com',
        )

    def test_new_env_contract_takes_precedence(self):
        env = {
            'B2_REGION': 'us-west-002',
            'B2_APPLICATION_KEY_ID': 'new-key-id',
            'B2_APPLICATION_KEY': 'new-key',
            'B2_BUCKET_NAME': 'public-bucket',
            'B2_PRIVATE_BUCKET_NAME': 'private-bucket',
            'B2_PRIVATE_APPLICATION_KEY_ID': 'private-key-id',
            'B2_PRIVATE_APPLICATION_KEY': 'private-key',
            'B2_WRITE_BUCKET_NAME': 'write-bucket',
            'B2_WRITE_APPLICATION_KEY_ID': 'write-key-id',
            'B2_WRITE_APPLICATION_KEY': 'write-key',
            'B2_TRANSIENT_BUCKET_NAME': 'copy-bucket',
            'KEY_ID_RO': 'legacy-key-id',
            'APPLICATION_KEY_RO': 'legacy-key',
        }
        with patch.dict(os.environ, env, clear=True):
            config = sample.load_b2_config()

        self.assertEqual(config.key_id, 'new-key-id')
        self.assertEqual(config.application_key, 'new-key')
        self.assertEqual(config.public_bucket_name, 'public-bucket')
        self.assertEqual(config.private_bucket_name, 'private-bucket')
        self.assertEqual(config.write_bucket_name, 'write-bucket')
        self.assertEqual(config.transient_bucket_name, 'copy-bucket')
        self.assertEqual(
            config.public_url_base,
            'https://s3.us-west-002.backblazeb2.com/public-bucket',
        )

    def test_legacy_env_contract_still_loads(self):
        env = {
            'ENDPOINT': 'https://s3.us-west-002.backblazeb2.com',
            'KEY_ID_RO': 'legacy-key-id',
            'APPLICATION_KEY_RO': 'legacy-key',
            'KEY_ID_PRIVATE_RO': 'legacy-private-key-id',
            'APPLICATION_KEY_PRIVATE_RO': 'legacy-private-key',
        }
        with patch.dict(os.environ, env, clear=True):
            config = sample.load_b2_config()

        self.assertEqual(config.key_id, 'legacy-key-id')
        self.assertEqual(config.private_key_id, 'legacy-private-key-id')
        self.assertEqual(config.public_bucket_name, sample.DEFAULT_PUBLIC_BUCKET_NAME)
        self.assertEqual(config.private_bucket_name, sample.DEFAULT_PRIVATE_BUCKET_NAME)

    def test_private_bucket_defaults_to_public_without_private_identity(self):
        env = {
            'B2_REGION': 'us-west-002',
            'B2_APPLICATION_KEY_ID': 'key-id',
            'B2_APPLICATION_KEY': 'key',
            'B2_BUCKET_NAME': 'public-bucket',
        }
        with patch.dict(os.environ, env, clear=True):
            config = sample.load_b2_config()

        self.assertEqual(config.private_bucket_name, 'public-bucket')
        self.assertEqual(config.private_key_id, 'key-id')

    def test_missing_config_names_expected_contracts(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'B2_REGION or ENDPOINT'):
                sample.load_b2_config()


class B2SampleOperationTests(unittest.TestCase):
    def test_public_url_base_is_object_prefix(self):
        b2 = FakeB2(['album/photo.jpg'])
        urls = list(sample.iter_objects_browsable_url(
            'my-bucket',
            'https://f005.backblazeb2.com/file/my-bucket',
            b2,
        ))

        self.assertEqual(
            urls,
            ['https://f005.backblazeb2.com/file/my-bucket/album/photo.jpg'],
        )

    def test_delete_versions_batches_at_b2_limit(self):
        client = FakeClient()

        with redirect_stdout(StringIO()):
            sample.delete_files_all_versions('bucket', ['photo.jpg'], client)

        self.assertEqual(len(client.delete_calls), 2)
        self.assertEqual(len(client.delete_calls[0]), sample.B2_DELETE_BATCH_SIZE)
        self.assertEqual(len(client.delete_calls[1]), 1)


if __name__ == '__main__':
    unittest.main()
