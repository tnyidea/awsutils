import datetime
import dataclasses
import typing

import boto3
import pytz

import awsutils


@dataclasses.dataclass
class S3Object:
    """S3Object class"""
    aws_session: boto3.session.Session = None
    region: str = ''
    bucket: str = ''
    object_key: str = ''
    file_extension: str = ''
    file_type: str = ''
    exists: bool = False
    etag: str = ''
    size: int = 0
    storage_class: str = ''
    last_modified: datetime.datetime = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
    event_name: str = ''

    def _list_object_v2(self) -> [typing.Optional[Exception]]:
        try:
            s3_client = self.aws_session.client('s3')
        except Exception as e:
            return e

        try:
            response = s3_client.list_objects_v2(
                Bucket=self.bucket,
                MaxKeys=1,
                Prefix=self.object_key,
            )
        except Exception as e:
            return e

        if response['KeyCount'] != 1:
            self.exists = False
            return None

        response_item = response['Contents'][0]
        self.exists = True
        self.etag = response_item['ETag']
        self.size = response_item['Size']
        self.storage_class = response_item['StorageClass']
        self.last_modified = response_item['LastModified']

        return None

    def copy(self, target, acl: str = '') -> [typing.Optional[Exception]]:
        try:
            s3_client = self.aws_session.client('s3')
            _ = s3_client.copy_object(
                ACL=acl,
                CopySource=f'/{self.bucket}/{self.object_key}',
                Bucket=target.bucket,
                Key=target.key,
            )
        except Exception as e:
            return e

        return None

    def download_bytes(self) -> [bytes, typing.Optional[Exception]]:
        try:
            s3_client = self.aws_session.client('s3')
            response = s3_client.get_object(Bucket=self.bucket, Key=self.object_key)
            b = response['Body'].read()
        except Exception as e:
            return None, e

        return b, None

    def upload_bytes(self, b: bytes) -> [typing.Optional[Exception]]:
        try:
            s3_client = self.aws_session.client('s3')
            _ = s3_client.put_object(Bucket=self.bucket, Key=self.object_key, Body=b)
            self._list_object_v2()
        except Exception as e:
            return e

        return None


def new_s3_object(aws_session: boto3.session.Session, bucket: str, object_key: str) \
        -> [awsutils.S3Object, typing.Optional[Exception]]:
    s3_object = S3Object()
    s3_object._aws_session = aws_session
    s3_object.bucket = bucket
    s3_object.object_key = object_key

    tokens = object_key.split('.')
    if len(tokens) > 1:
        file_extension = tokens[len(tokens) - 1]
        s3_object.file_extension = f'.{file_extension}'
        s3_object.file_type = f'.{file_extension.lower()}'

        bucket_region, e = awsutils.get_bucket_region(s3_object.aws_session, s3_object.bucket)
        if e is not None:
            return None, e

        s3_object.region = bucket_region

        e = s3_object._list_object_v2()
        if e is not None:
            return None, e


def new_s3_object_from_s3_url(aws_session: boto3.session.Session, url: str) \
        -> [awsutils.S3Object, typing.Optional[Exception]]:
    bucket, object_key = awsutils.split_s3_url(url)

    return new_s3_object(aws_session, bucket, object_key)
