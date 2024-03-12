import dataclasses

import botocore.exceptions

from .session import Session


@dataclasses.dataclass
class S3Bucket:
    """S3Object class"""
    session: Session = None
    region: str = ''
    bucket: str = ''
    prefix: str = ''

    def exists(self) -> bool:
        try:
            s3_client = self.session.s3_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html
            _ = s3_client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            print(e.response)
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e
        except Exception as e:
            raise e

        return True

    def delete(self) -> None:
        try:
            s3_client = self.session.s3_client()

            _ = s3_client.delete_bucket(
                Bucket=self.bucket,
            )
        except Exception as e:
            raise e

        return None

    def s3_url(self) -> str:
        if self.bucket == '':
            raise ValueError('invalid S3Bucket: undefined bucket name')

        if self.prefix == '':
            return f's3://{self.bucket}'

        return f's3://{self.bucket}/{self.prefix}'

    def _head_bucket(self) -> None:
        try:
            s3_client = self.session.s3_client()

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html
            response = s3_client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            raise e

        self.region = response['BucketRegion']

        return None


def create_s3_bucket(aws_session: Session, bucket: str) -> S3Bucket:
    s3_client = aws_session.s3_client()

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
        _ = s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                'LocationConstraint': aws_session.boto3_session.region_name,
            },
        )
    except Exception as e:
        raise e

    return new_s3_bucket(aws_session, bucket)


def new_s3_bucket(aws_session: Session, bucket: str, prefix: str = '') -> S3Bucket:
    s3_bucket = S3Bucket()
    s3_bucket.session = aws_session
    s3_bucket.bucket = bucket
    s3_bucket.prefix = prefix

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html
        s3_bucket._head_bucket()
    except Exception as e:
        raise e

    return s3_bucket


def get_bucket_region(aws_session: Session, bucket: str) -> str:
    try:
        s3_bucket = new_s3_bucket(aws_session, bucket)
    except Exception as e:
        raise e

    return s3_bucket.region
