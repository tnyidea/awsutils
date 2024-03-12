import datetime
import dataclasses

import boto3
import botocore.exceptions
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
    etag: str = ''
    size: int = 0
    storage_class: str = ''
    last_modified: datetime.datetime = datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
    event_name: str = ''

    def exists(self) -> bool:
        try:
            s3_client = self.session.s3_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
            _ = s3_client.head_object(
                Bucket=self.bucket,
                Key=self.object_key,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e
        except Exception as e:
            raise e

        return True

    def s3_url(self):
        if self.bucket == '' or self.object_key == '':
            return '', ValueError('invalid S3Object: must specify both bucket and object_key values')

        return f's3://{self.bucket}/{self.object_key}', None

    def _head_object(self) -> None:
        s3_client = self.session.s3_client()

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
            response = s3_client.head_object(
                Bucket=self.bucket,
                Key=self.object_key,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            else:
                raise e
        except Exception as e:
            raise e

        self.etag = response['ETag']
        self.size = response['ContentLength']
        # self.storage_class = response_item['StorageClass']
        self.last_modified = response['LastModified']
        self.content_type = response['ContentType']

        return None

    def copy(self, target, acl: str = '') -> None:
        s3_client = self.session.s3_client()

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/copy_object.html
            _ = s3_client.copy_object(
                ACL=acl,
                CopySource=f'/{self.bucket}/{self.object_key}',
                Bucket=target.bucket,
                Key=target.key,
            )
        except Exception as e:
            raise e

        return None

    def download_bytes(self) -> bytes:
        s3_client = self.session.s3_client()

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
            response = s3_client.get_object(
                Bucket=self.bucket,
                Key=self.object_key,
            )
            b = response['Body'].read()
        except Exception as e:
            raise e

        return b

    def upload_bytes(self, b: bytes) -> None:
        s3_client = self.session.s3_client()

        try:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html
            _ = s3_client.put_object(
                Bucket=self.bucket,
                Key=self.object_key,
                Body=b,
            )
            self._head_object()
        except Exception as e:
            raise e

        return None


def new_s3_object_from_s3_url(aws_session: boto3.session.Session, url: str) -> S3Object:
    try:
        split_url = split_s3_url(url)
    except Exception as e:
        raise e

    return new_s3_object(aws_session, split_url['bucket'], split_url['object_key'])


def new_s3_object(aws_session: Session, bucket: str, object_key: str) -> S3Object:
    s3_object = S3Object()
    s3_object.session = aws_session
    s3_object.bucket = bucket
    s3_object.object_key = object_key

    tokens = object_key.split('.')
    if len(tokens) > 1:
        file_extension = tokens[len(tokens) - 1]
        s3_object.file_extension = f'.{file_extension}'
        s3_object.file_type = f'.{file_extension.lower()}'

        try:
            bucket_region = get_bucket_region(s3_object.session, s3_object.bucket)
            s3_object._head_object()
        except Exception as e:
            raise e
        s3_object.region = bucket_region

    return s3_object
