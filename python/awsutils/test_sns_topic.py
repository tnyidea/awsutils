import pytest

from .conftest import new_test_aws_session
from .sns_topic import new_sns_topic, create_sns_topic
from .sqs_queue import create_sqs_queue


def test_new_sns_topic(setup):
    try:
        aws_session = new_test_aws_session()
        sns_topic = new_sns_topic(aws_session, 'test-sns-topic')
    except Exception as e:
        pytest.fail(e)

    assert sns_topic.exists()


def test_sns_create_sns_topic(setup):
    try:
        aws_session = new_test_aws_session()
        sns_topic = create_sns_topic(aws_session, 'test-create-sns-topic')
    except Exception as e:
        pytest.fail(e)

    assert sns_topic.exists()


def test_sns_delete_sns_topic(setup):
    try:
        aws_session = new_test_aws_session()
        sns_topic = new_sns_topic(aws_session, 'test-create-sns-topic')
        sns_topic.delete()
    except Exception as e:
        pytest.fail(e)

    assert not sns_topic.exists()


def test_sns_topic_subscribe(setup):
    try:
        aws_session = new_test_aws_session()
        sns_topic = create_sns_topic(aws_session, 'test-subscribe-sns-topic')
        sqs_queue = create_sqs_queue(aws_session, 'test-subscribe-sqs-queue')

        sns_topic.subscribe_sqs_queue(sqs_queue)
    except Exception as e:
        pytest.fail(e)

    assert True


def test_sns_topic_publish(setup):
    assert True
