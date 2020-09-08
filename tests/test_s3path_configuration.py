import boto3

from s3path import S3Path, register_configuration_parameter, _s3_accessor
from . import s3_mock


def test_hierarchical_configuration():
    path = S3Path('/foo/')
    register_configuration_parameter(path, parameters={'ContentType': 'text/html'})
    assert path in _s3_accessor.configuration_map
    assert _s3_accessor.configuration_map[path] == {'ContentType': 'text/html'}


def test_boto_methods_with_configuration(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')

    bucket = S3Path('/test-bucket/')
    register_configuration_parameter(bucket, parameters={'ContentType': 'text/html'})
    key = bucket.joinpath('bar.html')
    key.write_text('hello')
