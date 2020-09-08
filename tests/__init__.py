import boto3
import pytest
from moto import mock_s3

from s3path import _s3_accessor


@pytest.fixture()
def s3_mock():
    with mock_s3():
        _s3_accessor.s3 = boto3.resource('s3')
        yield
