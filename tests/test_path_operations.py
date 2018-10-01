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


def test_not_supported(monkeypatch):
    monkeypatch.setattr(S3Path._flavour, 'is_supported', False)
    error_message = f'cannot instantiate {S3Path.__name__} on your system'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path()


def test_cwd():
    error_message = f'{S3Path.__name__}.cwd() is unsupported on S3 service'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.cwd()


def test_home():
    error_message = f'{S3Path.__name__}.home() is unsupported on S3 service'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.home()


def test_chmod():
    error_message = f'{S3Path.__name__}.chmod() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.chmod(0o666)


def test_group():
    error_message = f'{S3Path.__name__}.group() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.group()


def test_is_mount():
    error_message = f'{S3Path.__name__}.is_mount() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_mount()


def test_is_symlink():
    error_message = f'{S3Path.__name__}.is_symlink() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_symlink()


def test_is_socket():
    error_message = f'{S3Path.__name__}.is_socket() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_socket()


def test_is_fifo():
    error_message = f'{S3Path.__name__}.is_fifo() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_fifo()


def test_is_block_device():
    error_message = f'{S3Path.__name__}.is_block_device() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_block_device()


def test_is_char_device():
    error_message = f'{S3Path.__name__}.is_char_device() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_char_device()


def test_lstat():
    error_message = f'{S3Path.__name__}.lstat() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.lstat()


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
