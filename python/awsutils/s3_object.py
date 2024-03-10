import datetime
import dataclasses
import typing

import boto3
import pytz

from .s3_bucket import get_bucket_region
from .s3_url import split_s3_url
from .session import Session


@dataclasses.dataclass
class S3Object:
    """S3Object class"""
    session: Session = None
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

    def s3_url(self):
        if self.bucket == '' or self.object_key == '':
            return '', ValueError('invalid S3Object: must specify both bucket and object_key values')

        return f's3://{self.bucket}/{self.object_key}', None

    def _list_object_v2(self) -> [typing.Optional[Exception]]:
        s3_client, e = self.session.s3_client()
        if e is not None:
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
        s3_client, e = self.session.s3_client()
        if e is not None:
            return e

        try:
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
        s3_client, e = self.session.s3_client()
        if e is not None:
            return e

        try:
            response = s3_client.get_object(Bucket=self.bucket, Key=self.object_key)
            b = response['Body'].read()
        except Exception as e:
            return None, e

        return b, None

    def upload_bytes(self, b: bytes) -> [typing.Optional[Exception]]:
        s3_client, e = self.session.s3_client()
        if e is not None:
            return e

        try:
            _ = s3_client.put_object(Bucket=self.bucket, Key=self.object_key, Body=b)
            self._list_object_v2()
        except Exception as e:
            return e

        return None


def new_s3_object_from_s3_url(aws_session: boto3.session.Session, url: str) -> S3Object:
    bucket, object_key = split_s3_url(url)

    return new_s3_object(aws_session, bucket, object_key)


def new_s3_object(aws_session: Session, bucket: str, object_key: str) -> S3Object:
    s3_object = S3Object()
    s3_object.aws_session = aws_session
    s3_object.bucket = bucket
    s3_object.object_key = object_key

    tokens = object_key.split('.')
    if len(tokens) > 1:
        file_extension = tokens[len(tokens) - 1]
        s3_object.file_extension = f'.{file_extension}'
        s3_object.file_type = f'.{file_extension.lower()}'

        bucket_region, e = get_bucket_region(s3_object.aws_session, s3_object.bucket)
        if e is not None:
            return None, e

        s3_object.region = bucket_region

        e = s3_object._list_object_v2()
        if e is not None:
            return None, e

    return s3_object, None
