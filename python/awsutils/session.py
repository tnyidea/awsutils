import dataclasses

import boto3.session


@dataclasses.dataclass
class Session:
    """Session class"""
    boto3_session: boto3.session.Session = None
    boto3_config: boto3.session.Config = None
    endpoint_url: str = None

    def s3_client(self):
        return self._client('s3')

    def _client(self, service: str):
        try:
            client = self.boto3_session.client(service, endpoint_url=self.endpoint_url, config=self.boto3_config)
        except Exception as e:
            raise e

        return client


def new_session_for_region(region: str):
    return new_session_from_config(boto3.session.Config(
        region_name=region,
    ))


def new_session_from_config(config: boto3.session.Config, endpoint_url: str = None):
    session = Session()
    session.boto3_config = config
    session.endpoint_url = endpoint_url

    try:
        aws_session = boto3.session.Session(region_name=getattr(config, 'region_name'))
    except Exception as e:
        raise e
    session.boto3_session = aws_session

    return session
