import boto3
import pytest
from moto import mock_s3

from s3path import register_configuration_parameter, PureS3Path, _s3_accessor


def _cleanup():
    _s3_accessor.configuration_map.get_configuration.cache_clear()
    _s3_accessor.configuration_map.get_general_options.cache_clear()
    _s3_accessor.configuration_map.is_setup = False


@pytest.fixture()
def reset_configuration_cache():
    try:
        _cleanup()
        yield
    finally:
        _cleanup()


@pytest.fixture()
def s3_mock(reset_configuration_cache):
    with mock_s3():
        register_configuration_parameter(PureS3Path('/'), resource=boto3.resource('s3'))
        yield


@pytest.fixture()
def enable_old_glob():
    register_configuration_parameter(PureS3Path('/'), glob_new_algorithm=False)
    yield
    register_configuration_parameter(PureS3Path('/'), glob_new_algorithm=True)
