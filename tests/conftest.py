import boto3
import pytest
from moto import mock_s3

from s3path import register_configuration_parameter, PureS3Path, _s3_accessor


@pytest.fixture()
def reset_configuration_cache():
    try:
        _s3_accessor.configuration_map.get_configuration.cache_clear()
        yield
    finally:
        _s3_accessor.configuration_map.get_configuration.cache_clear()


@pytest.fixture()
def s3_mock(reset_configuration_cache):
    with mock_s3():
        register_configuration_parameter(PureS3Path('/'), resource=boto3.resource('s3'))
        yield
