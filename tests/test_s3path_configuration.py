from s3path import S3Path, register_configuration_parameter, _s3_accessor


def test_hierarchical_configuration():
    path = S3Path('/foo/')
    register_configuration_parameter(path, parameters={'ContentType': 'text/html'})
    assert _s3_accessor.configuration_map == {path: {'ContentType': 'text/html'}}
