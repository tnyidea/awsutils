import boto3.session


def new_aws_session_for_region(region: str) -> boto3.session.Session:
    return boto3.session.Session(region_name=region)
