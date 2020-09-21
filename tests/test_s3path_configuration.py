import boto3
from botocore.client import Config

from s3path import S3Path, PureS3Path, register_configuration_parameter, _s3_accessor


def test_hierarchical_configuration(reset_configuration_cache):
    path = S3Path('/foo/')
    register_configuration_parameter(path, parameters={'ContentType': 'text/html'})
    assert path in _s3_accessor.configuration_map.arguments
    assert path not in _s3_accessor.configuration_map.resources
    assert _s3_accessor.configuration_map.get_configuration(path) == (
        _s3_accessor.configuration_map.default_resource, {'ContentType': 'text/html'})

    assert (_s3_accessor.configuration_map.get_configuration(S3Path('/foo/'))
            == _s3_accessor.configuration_map.get_configuration(PureS3Path('/foo/')))


def test_boto_methods_with_configuration(s3_mock, reset_configuration_cache):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')

    bucket = S3Path('/test-bucket/')
    register_configuration_parameter(bucket, parameters={'ContentType': 'text/html'})
    key = bucket.joinpath('bar.html')
    key.write_text('hello')


def test_configuration_per_bucket(reset_configuration_cache):
    local_stack_bucket_path = PureS3Path('/LocalStackBucket/')
    minio_bucket_path = PureS3Path('/MinIOBucket/')
    default_aws_s3_path = PureS3Path('/')

    register_configuration_parameter(
        default_aws_s3_path,
        parameters={'ContentType': 'text/html'})
    register_configuration_parameter(
        local_stack_bucket_path,
        parameters={},
        resource=boto3.resource('s3', endpoint_url='http://localhost:4566'))
    register_configuration_parameter(
        minio_bucket_path,
        parameters={'OutputSerialization': {'CSV': {}}},
        resource=boto3.resource(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'))

    assert _s3_accessor.configuration_map.get_configuration(PureS3Path('/')) == (
        _s3_accessor.configuration_map.default_resource, {'ContentType': 'text/html'})
    assert _s3_accessor.configuration_map.get_configuration(PureS3Path('/some_bucket')) == (
        _s3_accessor.configuration_map.default_resource, {'ContentType': 'text/html'})
    assert _s3_accessor.configuration_map.get_configuration(PureS3Path('/some_bucket')) == (
        _s3_accessor.configuration_map.default_resource, {'ContentType': 'text/html'})

    resources, arguments = _s3_accessor.configuration_map.get_configuration(minio_bucket_path)
    assert arguments == {'OutputSerialization': {'CSV': {}}}
    assert resources.meta.client._endpoint.host == 'http://localhost:9000'

    resources, arguments = _s3_accessor.configuration_map.get_configuration(minio_bucket_path / 'some_key')
    assert arguments == {'OutputSerialization': {'CSV': {}}}
    assert resources.meta.client._endpoint.host == 'http://localhost:9000'

    resources, arguments = _s3_accessor.configuration_map.get_configuration(local_stack_bucket_path)
    assert arguments == {}
    assert resources.meta.client._endpoint.host == 'http://localhost:4566'

    resources, arguments = _s3_accessor.configuration_map.get_configuration(local_stack_bucket_path / 'some_key')
    assert arguments == {}
    assert resources.meta.client._endpoint.host == 'http://localhost:4566'

