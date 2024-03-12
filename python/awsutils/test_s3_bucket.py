import os

import pytest

from .conftest import new_test_aws_session
from .s3_bucket import new_s3_bucket, create_s3_bucket, get_bucket_region


def test_new_s3_bucket(setup):
    try:
        aws_session = new_test_aws_session()
        s3_bucket = new_s3_bucket(aws_session, 'test-bucket')
    except Exception as e:
        pytest.fail(e)

    assert s3_bucket.exists()


def test_create_s3_bucket(setup):
    try:
        aws_session = new_test_aws_session()
        s3_bucket = create_s3_bucket(aws_session, 'test-create-s3-bucket')
    except Exception as e:
        pytest.fail(e)

    assert s3_bucket.exists()


def test_delete_s3_bucket(setup):
    try:
        aws_session = new_test_aws_session()
        s3_bucket = new_s3_bucket(aws_session, 'test-create-s3-bucket')
        s3_bucket.delete()
    except Exception as e:
        pytest.fail(e)

    assert not s3_bucket.exists()


def test_s3_bucket_s3_url(setup):
    try:
        aws_session = new_test_aws_session()
        s3_bucket = new_s3_bucket(aws_session, 'test-bucket')
    except Exception as e:
        pytest.fail(e)

    assert s3_bucket.s3_url() == 's3://test-bucket'
