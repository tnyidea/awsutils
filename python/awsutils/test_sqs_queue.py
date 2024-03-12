import pytest

from .conftest import new_test_aws_session
from .sqs_queue import new_sqs_queue, create_sqs_queue


def test_new_sqs_queue(setup):
    try:
        aws_session = new_test_aws_session()
        sqs_queue = new_sqs_queue(aws_session, 'test-sqs-queue')
    except Exception as e:
        pytest.fail(e)

    assert sqs_queue.exists()


def test_create_sqs_queue(setup):
    try:
        aws_session = new_test_aws_session()
        sqs_queue = create_sqs_queue(aws_session, 'test-create-sqs-queue')
    except Exception as e:
        pytest.fail(e)

    assert sqs_queue.exists()


def test_delete_sqs_queue(setup):
    try:
        aws_session = new_test_aws_session()
        sqs_queue = new_sqs_queue(aws_session, 'test-create-sqs-queue')
        sqs_queue.delete()
    except Exception as e:
        pytest.fail(e)

    assert not sqs_queue.exists()
