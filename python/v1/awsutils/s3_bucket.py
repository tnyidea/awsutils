import dataclasses
import typing

import boto3

import awsutils


@dataclasses.dataclass
class S3Bucket:
    """S3Object class"""
    aws_session: boto3.session.Session
    region: str
    bucket: str
    prefix: str

def new_s3_bucket(aws_session: boto3.session.Session, bucket: str, prefix: str=None) \
    -> [awsutils.S3Bucket, typing.Optional[Exception]]:

def get_bucket_region(aws_session: boto3.session.Session, bucket: str) \
        -> [str, typing.Optional[Exception]]:
    try:
        s3_client = aws_session.client('s3')
        response = s3_client.head_bucket(Bucket=bucket)
    except Exception as e:
        return '', e

    return response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region'], None
