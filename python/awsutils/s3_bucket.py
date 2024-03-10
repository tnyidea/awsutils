import dataclasses

from .session import Session


@dataclasses.dataclass
class S3Bucket:
    """S3Object class"""
    session: Session = None
    region: str = ''
    bucket: str = ''
    prefix: str = ''

    def s3_url(self) -> str:
        if self.bucket == '':
            raise ValueError('invalid S3Bucket: undefined bucket name')

        if self.prefix == '':
            return f's3://{self.bucket}'

        return f's3://{self.bucket}/{self.prefix}'


def new_s3_bucket(aws_session: Session, bucket: str, prefix: str = '') -> S3Bucket:
    s3_bucket = S3Bucket()
    s3_bucket.aws_session = aws_session
    s3_bucket.bucket = bucket
    s3_bucket.prefix = prefix

    try:
        s3_client = aws_session.s3_client()
        _ = s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                'LocationConstraint': getattr(aws_session.boto3_config, 'region_name'),
            },
        )
        bucket_region = get_bucket_region(s3_bucket.aws_session, s3_bucket.bucket)
    except Exception as e:
        raise e

    s3_bucket.region = bucket_region

    return s3_bucket


def get_bucket_region(aws_session: Session, bucket: str) -> str:
    try:
        s3_client = aws_session.s3_client()
        response = s3_client.head_bucket(Bucket=bucket)
    except Exception as e:
        raise e

    return response['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']
