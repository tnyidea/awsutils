import logging
import os

import boto3.session
import pytest
import testcontainers.localstack

from .s3_bucket import new_s3_bucket
from .session import Session, new_session_from_config


@pytest.fixture(scope='session')
def setup(request):
    os.environ['AWS_REGION'] = 'us-east-2'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'

    try:
        localstack_container = testcontainers.localstack.LocalStackContainer('localstack/localstack:latest')
        localstack_container.start()

        localstack_port = localstack_container.get_exposed_port(4566)
        aws_endpoint_url = f'http://localhost:{localstack_port}'
        os.environ['AWS_ENDPOINT_URL'] = aws_endpoint_url

        aws_session = new_session_from_config(
            boto3.session.Config(
                region_name=os.environ.get('AWS_DEFAULT_REGION'),
                s3={'addressing_style': 'path'}
            ),
            endpoint_url=os.environ.get('AWS_ENDPOINT_URL'),
        )

        setup_localstack_s3_objects(aws_session, './testdata/localstack/s3')
    except Exception as e:
        pytest.fail(f'error: setup_localstack_s3_object failed:{e}')


def setup_localstack_s3_objects(aws_session: Session, s3_path: str) -> None:
    logging.info("=== setup_localstack_s3_objects")

    try:
        _ = new_s3_bucket(aws_session, 'test-bucket')
    except Exception as e:
        raise e

    logging.info("=== setup_localstack_s3_objects complete")
