import sys
import boto3
import pytest
from moto import mock_aws

from s3path import register_configuration_parameter, PureS3Path


if sys.version_info >= (3, 12):
    from s3path import accessor

    def _cleanup():
        accessor.configuration_map.get_configuration.cache_clear()
        accessor.configuration_map.get_general_options.cache_clear()
        accessor.configuration_map.is_setup = False
else:
    from s3path import S3Path

    def _cleanup():
        S3Path._accessor.configuration_map.get_configuration.cache_clear()
        S3Path._accessor.configuration_map.get_general_options.cache_clear()
        S3Path._accessor.configuration_map.is_setup = False


@pytest.fixture()
def reset_configuration_cache():
    try:
        _cleanup()
        yield
    finally:
        _cleanup()


@pytest.fixture()
def s3_mock(reset_configuration_cache):
    with mock_aws():
        register_configuration_parameter(PureS3Path('/'), resource=boto3.resource('s3'))
        yield


@pytest.fixture()
def enable_old_glob():
    register_configuration_parameter(PureS3Path('/'), glob_new_algorithm=False)
    yield
    register_configuration_parameter(PureS3Path('/'), glob_new_algorithm=True)
