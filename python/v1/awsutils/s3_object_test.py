import datetime
import io
import unittest
import unittest.mock as mock

import boto3
import botocore.stub
import botocore.response
import freezegun
import pytz

import awsutils
from awsutils.s3_object import _head_bucket, _split_s3_url

ENCODING_UTF_8 = 'utf-8'


@mock.patch('boto3.session.Session')
@freezegun.freeze_time(datetime.datetime(2023, 10, 25, 1, 0, 0, 0, tzinfo=pytz.utc))
class TestMockS3Object(unittest.TestCase):
    def setUp(self):
        self.test_region = 'us-east-2'
        self.test_bucket = 'awsutils-test-bucket'
        self.test_object_key = 'test-object-key.json'
        self.test_body = 'test-body'

        self.mock_head_bucket_request = {
            'Bucket': self.test_bucket,
        }
        self.mock_head_bucket_response = {
            'ResponseMetadata': {
                'RequestId': 'DVA9AQ2WHMAKVTXX',
                'HostId': 'U8XUpy7ujyDkSIl2xdNi5ye78BvANf4wtW6zOixWOrMtNLYHmcuYiFhm/TPNG8ucud58hZ1TaoyMY7Xh7iOUCw==',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-id-2': 'U8XUpy7ujyDkSIl2xdNi5ye78BvANf4wtW6zOixWOrMtNLYHmcuYiFhm/TPNG8ucud58hZ1TaoyMY7Xh7iOUCw==',
                    'x-amz-request-id': 'DVA9AQ2WHMAKVTXX',
                    'date': 'Sun, 29 Oct 2023 00:34:46 GMT',
                    'x-amz-bucket-region': 'us-east-2',
                    'x-amz-access-point-alias': 'false',
                    'content-type': 'application/xml',
                    'server': 'AmazonS3'
                },
                'RetryAttempts': 0
            }
        }
        self.mock_get_object_request = {
            'Bucket': self.test_bucket,
            'Key': self.test_object_key,
        }
        self.mock_get_object_response = {
            'Body': botocore.response.StreamingBody(
                io.BytesIO(bytes(self.test_body, ENCODING_UTF_8)),
                len(self.test_body),
            ),
            'ETag': 'test-etag',
            'ContentLength': 1234,
            'StorageClass': 'test-storage-class',
            'LastModified': datetime.datetime.utcnow(),
        }
        self.mock_put_object_params = {
            'Bucket': self.test_bucket,
            'Key': self.test_object_key,
            'Body': bytes(self.test_body, ENCODING_UTF_8)
        }
        self.mock_put_object_response = {}
        self.mock_list_objects_v2_request = {
            'Bucket': self.test_bucket,
            'MaxKeys': 1,
            'Prefix': self.test_object_key,
        }
        self.mock_list_objects_v2_response = {
            'ResponseMetadata': {
                'RequestId': 'test-request-id',
                'HostId': 'test-host-id',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-id-2': 'test-amz-id-2',
                    'x-amz-request-id': 'test-amz-request-id',
                    'date': 'Wed, 25 Oct 2023 01:00:00 GMT',
                    'x-amz-bucket-region': 'us-east-2',
                    'content-type': 'application/xml',
                    'transfer-encoding': 'chunked',
                    'server': 'AmazonS3'
                },
                'RetryAttempts': 0
            },
            'IsTruncated': False,
            'Contents': [
                {
                    'Key': 'test-object-key.json',
                    'LastModified': datetime.datetime.now(),
                    'ETag': 'test-etag',
                    'Size': 1234,
                    'StorageClass': 'test-storage-class'
                }
            ],
            'Name': 'awsutils-test-bucket',
            'Prefix': 'test-object-key.json',
            'MaxKeys': 1,
            'EncodingType': 'url',
            'KeyCount': 1
        }

    def test__split_s3_url(self, _):
        bucket, object_key = _split_s3_url(f's3://{self.test_bucket}/{self.test_object_key}')

        self.assertEqual(bucket, self.test_bucket)
        self.assertEqual(object_key, self.test_object_key)

    def test__head_bucket(self, mock_boto3_session):
        client = boto3.client('s3')
        stubber = botocore.stub.Stubber(client)
        stubber.add_response('head_bucket', self.mock_head_bucket_response,
                             self.mock_head_bucket_request)
        stubber.activate()

        with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=client)):
            region = _head_bucket(mock_boto3_session, self.test_bucket)

            expected = 'us-east-2'
            self.assertEqual(expected, region)

    def test_new_s3_object(self, mock_boto3_session):
        mock_boto3_client = boto3.client('s3')
        stubber = botocore.stub.Stubber(mock_boto3_client)
        stubber.add_response('head_bucket', self.mock_head_bucket_response,
                             self.mock_head_bucket_request)
        stubber.add_response('list_objects_v2', self.mock_list_objects_v2_response,
                             self.mock_list_objects_v2_request)
        stubber.activate()

        with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
            s3_object = awsutils.S3Object(mock_boto3_session, self.test_bucket, self.test_object_key)

            expected = {
                '_aws_session': mock_boto3_session,
                'bucket': 'awsutils-test-bucket',
                'etag': 'test-etag',
                'exists': True,
                'file_extension': '.json',
                'file_type': '.json',
                'last_modified': datetime.datetime.utcnow(),
                'object_key': 'test-object-key.json',
                'region': 'us-east-2',
                'size': 1234,
                'source_event_type': '',
                'storage_class': 'test-storage-class'
            }
            self.assertEqual(expected, s3_object.__dict__)

    def test_new_s3_object_from_s3_url(self, mock_boto3_session):
        mock_boto3_client = boto3.client('s3')
        stubber = botocore.stub.Stubber(mock_boto3_client)
        stubber.add_response('head_bucket', self.mock_head_bucket_response,
                             self.mock_head_bucket_request)
        stubber.add_response('list_objects_v2', self.mock_list_objects_v2_response,
                             self.mock_list_objects_v2_request)
        stubber.activate()

        with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
            s3_object = awsutils.S3Object.from_s3_url(mock_boto3_session,
                                                      f's3://{self.test_bucket}/{self.test_object_key}')
            expected = {
                '_aws_session': mock_boto3_session,
                'bucket': 'awsutils-test-bucket',
                'etag': 'test-etag',
                'exists': True,
                'file_extension': '.json',
                'file_type': '.json',
                'last_modified': datetime.datetime.utcnow(),
                'object_key': 'test-object-key.json',
                'region': 'us-east-2',
                'size': 1234,
                'source_event_type': '',
                'storage_class': 'test-storage-class'
            }
            self.assertEqual(expected, s3_object.__dict__)

    def test_download_bytes(self, mock_boto3_session):
        mock_boto3_client = boto3.client('s3')
        stubber = botocore.stub.Stubber(mock_boto3_client)
        stubber.add_response('head_bucket', self.mock_head_bucket_response,
                             self.mock_head_bucket_request)
        stubber.add_response('list_objects_v2', self.mock_list_objects_v2_response,
                             self.mock_list_objects_v2_request)
        stubber.add_response('get_object', self.mock_get_object_response,
                             self.mock_get_object_request)
        stubber.activate()

        with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
            s3_object = awsutils.S3Object(mock_boto3_session, self.test_bucket, self.test_object_key)
            b = s3_object.download_bytes()

            self.assertEqual(bytes(self.test_body, ENCODING_UTF_8), b)

    def test_upload_bytes(self, mock_boto3_session):
        mock_boto3_client = boto3.client('s3')
        stubber = botocore.stub.Stubber(mock_boto3_client)
        stubber.add_response('head_bucket', self.mock_head_bucket_response,
                             self.mock_head_bucket_request)
        stubber.add_response('list_objects_v2', self.mock_list_objects_v2_response,
                             self.mock_list_objects_v2_request)
        stubber.add_response('put_object', self.mock_put_object_response,
                             self.mock_put_object_params)
        stubber.add_response('list_objects_v2', self.mock_list_objects_v2_response,
                             self.mock_list_objects_v2_request)
        stubber.activate()

        with mock.patch('boto3.session.Session.client', mock.MagicMock(return_value=mock_boto3_client)):
            s3_object = awsutils.S3Object(mock_boto3_session, self.test_bucket, self.test_object_key)
            # expected = {
            #     '_aws_session': mock_boto3_session,
            #     'bucket': 'awsutils-test-bucket',
            #     'etag': '',
            #     'exists': False,
            #     'file_extension': '.json',
            #     'file_type': '.json',
            #     'last_modified': datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc),
            #     'object_key': 'test-object-key.json',
            #     'region': 'us-east-2',
            #     'size': 0,
            #     'source_event_type': '',
            #     'storage_class': ''
            # }
            # self.assertEqual(expected, s3_object.__dict__)

            s3_object.upload_bytes(bytes(self.test_body, ENCODING_UTF_8))
            expected = {
                '_aws_session': mock_boto3_session,
                'bucket': 'awsutils-test-bucket',
                'etag': 'test-etag',
                'exists': True,
                'file_extension': '.json',
                'file_type': '.json',
                'last_modified': datetime.datetime.utcnow(),
                'object_key': 'test-object-key.json',
                'region': 'us-east-2',
                'size': 1234,
                'source_event_type': '',
                'storage_class': 'test-storage-class'
            }
            self.assertEqual(expected, s3_object.__dict__)


class TestS3Object(unittest.TestCase):
    def setUp(self):
        self.test_region = 'us-east-2'
        self.test_bucket = 'awsutils-test-bucket'
        self.test_object_key = 'test-object-key.json'
        self.test_body = 'test-body'

    def test__head_bucket(self):
        aws_session = awsutils.new_aws_session_for_region('us-east-2')
        region = _head_bucket(aws_session, self.test_bucket)

        expected = 'us-east-2'
        self.assertEqual(expected, region)

    def test_new_s3_object(self):
        aws_session = awsutils.new_aws_session_for_region(self.test_region)
        s3_object = awsutils.S3Object(aws_session, self.test_bucket, self.test_object_key)
        print(s3_object.__dict__)

    def test_new_s3_object_from_s3_url(self):
        aws_session = awsutils.new_aws_session_for_region(self.test_region)
        s3_object = awsutils.S3Object.from_s3_url(aws_session, f's3://{self.test_bucket}/{self.test_object_key}')
        print(s3_object.__dict__)

    def test_download_bytes(self):
        aws_session = awsutils.new_aws_session_for_region(self.test_region)
        s3_object = awsutils.S3Object(aws_session, self.test_bucket, self.test_object_key)
        b = s3_object.download_bytes()
        print(b.decode(ENCODING_UTF_8))

    def test_upload_bytes(self):
        aws_session = awsutils.new_aws_session_for_region(self.test_region)
        s3_object = awsutils.S3Object(aws_session, self.test_bucket, 'ggb' + self.test_object_key)
        s3_object.upload_bytes(bytes(self.test_body, ENCODING_UTF_8))


if __name__ == '__main__':
    unittest.main()
