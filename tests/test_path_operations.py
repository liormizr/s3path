from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import pytest
from moto import mock_s3

from s3path import PureS3Path, S3Path, StatResult, _s3_accessor


@pytest.fixture()
def s3_mock():
    with mock_s3():
        _s3_accessor.s3 = boto3.resource('s3')
        yield


def test_path_support():
    assert PureS3Path in S3Path.mro()
    assert Path in S3Path.mro()


def test_stat(s3_mock):
    path = S3Path('fake-bucket/fake-key')
    with pytest.raises(ValueError):
        path.stat()

    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(ClientError):
        path.stat()

    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'Test.test')
    simple_object.put(Body=b'test data')

    path = S3Path('/test-bucket/Test.test')
    stat = path.stat()
    assert isinstance(stat, StatResult)
    assert stat == StatResult(
        size=simple_object.size,
        last_modified=simple_object.last_modified,
    )

    path = S3Path('/test-bucket')
    assert path.stat() is None


def test_exists(s3_mock):
    path = S3Path('./fake-key')
    with pytest.raises(ValueError):
        path.exists()

    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(ClientError):
        path.exists()

    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    assert not S3Path('/test-bucket/Test.test').exists()
    path = S3Path('/test-bucket/directory/Test.test')
    assert path.exists()
    for parent in path.parents:
        assert parent.exists()


def test_glob(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    assert list(S3Path('/test-bucket/').glob('*.test')) == []
    assert list(S3Path('/test-bucket/directory/').glob('*.test')) == [S3Path('/test-bucket/directory/Test.test')]
    assert list(S3Path('/test-bucket/').glob('**/*.test')) == [S3Path('/test-bucket/directory/Test.test')]
    assert list(S3Path('/test-bucket/').rglob('*.test')) == [S3Path('/test-bucket/directory/Test.test')]
    assert list(S3Path('/test-bucket/').rglob('**/*.test')) == [S3Path('/test-bucket/directory/Test.test')]

    simple_object = s3.ObjectSummary('test-bucket', 'pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'setup.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    simple_object.put(Body=b'test data')

    assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*.py')) == [
        S3Path('/test-bucket/pathlib.py'),
        S3Path('/test-bucket/setup.py'),
        S3Path('/test-bucket/test_pathlib.py')]
    assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*/*.py')) == [S3Path('/test-bucket/docs/conf.py')]
    assert sorted(S3Path.from_uri('s3://test-bucket/').glob('**/*.py')) == [
        S3Path('/test-bucket/build/lib/pathlib.py'),
        S3Path('/test-bucket/docs/conf.py'),
        S3Path('/test-bucket/pathlib.py'),
        S3Path('/test-bucket/setup.py'),
        S3Path('/test-bucket/test_pathlib.py')]


def test_is_dir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'setup.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    simple_object.put(Body=b'test data')

    assert not S3Path('/test-bucket/fake.test').is_dir()
    assert not S3Path('/test-bucket/fake/').is_dir()
    assert S3Path('/test-bucket/directory').is_dir()
    assert not S3Path('/test-bucket/directory/Test.test').is_dir()
    assert not S3Path('/test-bucket/pathlib.py').is_dir()
    assert not S3Path('/test-bucket/docs/conf.py').is_dir()
    assert S3Path('/test-bucket/docs/').is_dir()
    assert S3Path('/test-bucket/build/').is_dir()
    assert S3Path('/test-bucket/build/lib').is_dir()
    assert not S3Path('/test-bucket/build/lib/pathlib.py').is_dir()


def test_is_file(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'setup.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    simple_object.put(Body=b'test data')

    assert not S3Path('/test-bucket/fake.test').is_file()
    assert not S3Path('/test-bucket/fake/').is_file()
    assert not S3Path('/test-bucket/directory').is_file()
    assert S3Path('/test-bucket/directory/Test.test').is_file()
    assert S3Path('/test-bucket/pathlib.py').is_file()
    assert S3Path('/test-bucket/docs/conf.py').is_file()
    assert not S3Path('/test-bucket/docs/').is_file()
    assert not S3Path('/test-bucket/build/').is_file()
    assert not S3Path('/test-bucket/build/lib').is_file()
    assert S3Path('/test-bucket/build/lib/pathlib.py').is_file()


def test_iterdir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'setup.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/make.bat')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/index.rst')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/Makefile')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/_templates/11conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/_build/22conf.py')
    simple_object.put(Body=b'test data')
    simple_object = s3.ObjectSummary('test-bucket', 'docs/_static/conf.py')
    simple_object.put(Body=b'test data')

    s3_path = S3Path('/test-bucket/docs')
    assert sorted(s3_path.iterdir()) == [
        S3Path('/test-bucket/docs/Makefile'),
        S3Path('/test-bucket/docs/_build'),
        S3Path('/test-bucket/docs/_static'),
        S3Path('/test-bucket/docs/_templates'),
        S3Path('/test-bucket/docs/conf.py'),
        S3Path('/test-bucket/docs/index.rst'),
        S3Path('/test-bucket/docs/make.bat'),
    ]


def test_open(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    file_obj = path.open()
    assert file_obj.read() == 'test data'


def test_open_binary_read(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    with path.open(mode='br') as file_obj:
        assert file_obj.readlines() == [b'test data']

    with path.open(mode='rb') as file_obj:
        assert file_obj.readline() == b'test data'
        assert file_obj.readline() == b''
        assert file_obj.readline() == b''

    assert path.read_bytes() == b'test data'


def test_open_text_read(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    with path.open(mode='r') as file_obj:
        assert file_obj.readlines() == ['test data']

    with path.open(mode='rt') as file_obj:
        assert file_obj.readline() == 'test data'
        assert file_obj.readline() == ''
        assert file_obj.readline() == ''

    assert path.read_text() == 'test data'


@pytest.mark.skip()
def test_owner(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    simple_object = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    simple_object.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    assert path.owner() == '???'
