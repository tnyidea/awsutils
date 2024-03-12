import logging
import os

import boto3.session
import pytest
import testcontainers.localstack

from .s3_bucket import create_s3_bucket
from .s3_object import new_s3_object
from .session import Session, new_session_from_config
from .sns_topic import create_sns_topic
from .sqs_queue import create_sqs_queue

localstack_container = testcontainers.localstack.LocalStackContainer('localstack/localstack:latest')


@pytest.fixture(scope='session')
def setup(request):
    def finalizer():
        localstack_container.stop()

    request.addfinalizer(finalizer)

    os.environ['AWS_REGION'] = 'us-east-2'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'

    try:
        localstack_container.start()

        localstack_port = localstack_container.get_exposed_port(4566)
        aws_endpoint_url = f'http://localhost:{localstack_port}'
        os.environ['AWS_ENDPOINT_URL'] = aws_endpoint_url

        aws_session = new_test_aws_session()

        setup_localstack_s3_objects(aws_session, './testdata/localstack/s3')
        setup_localstack_sqs_queue(aws_session)
        setup_localstack_sns_topic(aws_session)
    except Exception as e:
        pytest.fail(f'error: setup_localstack_s3_object failed:{e}')


def new_test_aws_session() -> Session:
    try:
        aws_session = new_session_from_config(
            boto3.session.Config(
                region_name=os.environ.get('AWS_DEFAULT_REGION'),
                s3={'addressing_style': 'path'}
            ),
            endpoint_url=os.environ.get('AWS_ENDPOINT_URL'),
        )
    except Exception as e:
        raise e

    return aws_session


def setup_localstack_s3_objects(aws_session: Session, s3_path: str) -> None:
    logging.info("=== setup_localstack_s3_objects")

    try:
        entries = os.scandir(s3_path)

        for entry in entries:
            if not entry.is_dir():
                continue

            bucket = entry.name
            logging.info(f'--- Creating bucket: {bucket}')
            _ = create_s3_bucket(aws_session, bucket)

            bucket_path = f'{s3_path}/{bucket}'

            for root, _, files in os.walk(bucket_path, topdown=False):
                for filename in files:
                    file_path = os.path.join(root, filename)

                    f = open(file_path, mode="rb")
                    b = f.read()

                    object_key = file_path.replace(f'{bucket_path}/', '')

                    logging.info(f'--- Creating object: {object_key}')
                    s3_object = new_s3_object(aws_session, bucket, object_key)
                    s3_object.upload_bytes(b)
    except Exception as e:
        raise e

    logging.info("=== setup_localstack_s3_objects complete")


def setup_localstack_sns_topic(aws_session: Session) -> None:
    logging.info('=== setup_localstack_sns_topic')

    try:
        sqs_queue = create_sqs_queue(aws_session, 'test-sns-queue')
        sns_topic = create_sns_topic(aws_session, 'test-sns-arn')
        _ = sns_topic.subscribe_sqs_queue(sqs_queue)
    except Exception as e:
        raise e

    logging.info('=== setup_localstack_sns_topic complete')


def setup_localstack_sqs_queue(aws_session: Session) -> None:
    logging.info('=== setup_localstack_sqs_queue')

    try:
        _ = create_sqs_queue(aws_session, 'test-sqs-queue')
    except Exception as e:
        raise e

    logging.info('=== setup_localstack_sqs_queue complete')
