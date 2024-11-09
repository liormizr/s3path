import shutil
import sys
from datetime import timedelta
from pathlib import Path
from io import UnsupportedOperation
from tempfile import NamedTemporaryFile

import boto3
import requests
from botocore.exceptions import ClientError
import pytest

from s3path import PureS3Path, S3Path, StatResult, VersionedS3Path

# todo: test samefile/touch method
# todo: test security and boto config changes


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
    object_summary = s3.ObjectSummary('test-bucket', 'Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/Test.test')
    stat = path.stat()

    assert isinstance(stat, StatResult)
    assert stat == StatResult(
        size=object_summary.size,
        last_modified=object_summary.last_modified,
    )

    with NamedTemporaryFile() as local_file:
        local_file.write(path.read_bytes())
        local_file.flush()
        local_path = Path(local_file.name)

        local_stat = local_path.stat()
        s3_stat = path.stat()

        assert s3_stat.st_size == local_stat.st_size == s3_stat.size
        assert s3_stat.last_modified.timestamp() == s3_stat.st_mtime
        assert s3_stat.st_mtime < local_stat.st_mtime

    with pytest.raises(UnsupportedOperation):
        path.stat().st_atime

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
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    assert not S3Path('/test-bucket/Test.test').exists()
    path = S3Path('/test-bucket/directory/Test.test')
    assert path.exists()
    for parent in path.parents:
        assert parent.exists()

    assert S3Path('/').exists()


def test_glob(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    assert list(S3Path('/test-bucket/').glob('*.test')) == []
    assert list(S3Path('/test-bucket/directory/').glob('*.test')) == [S3Path('/test-bucket/directory/Test.test')]
    assert list(S3Path('/test-bucket/').glob('**/*.test')) == [S3Path('/test-bucket/directory/Test.test')]

    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')

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
    assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*cs')) == [S3Path('/test-bucket/docs/')]
    assert sorted(S3Path.from_uri('s3://test-bucket/').glob('docs/')) == [S3Path('/test-bucket/docs/')]


def test_glob_nested_folders_issue_no_115(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='my-bucket')
    full_folder_tree = ''
    object_summary = s3.ObjectSummary('my-bucket', 'test.txt')
    object_summary.put(Body=b'test data')
    for folder in range(6):
        object_summary = s3.ObjectSummary('my-bucket', f'{full_folder_tree}test.txt')
        object_summary.put(Body=b'test data')
        full_folder_tree += f'{folder}/'

    bucket = S3Path("/my-bucket/")
    path = bucket
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/test.txt')]
    path /= S3Path('0/')
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/0/test.txt')]
    path /= S3Path('1/')
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/0/1/test.txt')]
    path /= S3Path('2/')
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/0/1/2/test.txt')]
    path /= S3Path('3/')
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/0/1/2/3/test.txt')]
    path /= S3Path('4/')
    assert list(path.glob('*.txt')) == [S3Path('/my-bucket/0/1/2/3/4/test.txt')]

    bucket = S3Path("/my-bucket/")
    path = bucket
    for index, folder in enumerate(range(6)):
        assert sum(1 for _ in path.rglob('*.txt')) == 6 - index
        path /= S3Path(f'{folder}/')


def test_glob_nested_folders_issue_no_120(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='my-bucket')
    object_summary = s3.ObjectSummary('my-bucket', 's3path-test/nested/further/test.txt')
    object_summary.put(Body=b'test data')

    path = S3Path("/my-bucket/s3path-test/nested/")
    assert list(path.glob("further/*")) == [S3Path('/my-bucket/s3path-test/nested/further/test.txt')]


def test_glob_old_algo(s3_mock, enable_old_glob):
    if sys.version_info > (3, 12):
        with pytest.deprecated_call():
            test_glob(s3_mock)
    else:
        test_glob(s3_mock)


def test_glob_issue_160(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='my-bucket')
    example_paths = [
        's3path/output',
        's3path/1/output',
        's3path/2/output',
        's3path/3/output',
    ]
    for example_path in example_paths:
        object_summary = s3.ObjectSummary('my-bucket', f'{example_path}/test.txt')
        object_summary.put(Body=b'test data')

    path = S3Path.from_uri("s3://my-bucket/s3path")
    assert set(path.glob('**/output/')) == {
        S3Path('/my-bucket/s3path/output'),
        S3Path('/my-bucket/s3path/1/output'),
        S3Path('/my-bucket/s3path/2/output'),
        S3Path('/my-bucket/s3path/3/output'),
    }
    assert sum(1 for _ in path.glob('**/output/')) == 4

    assert set(path.rglob('output/')) == {
        S3Path('/my-bucket/s3path/output'),
        S3Path('/my-bucket/s3path/1/output'),
        S3Path('/my-bucket/s3path/2/output'),
        S3Path('/my-bucket/s3path/3/output'),
    }
    assert sum(1 for _ in path.rglob('output/')) == 4


def test_glob_issue_160_weird_behavior(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='my-bucket')

    first_dir = S3Path.from_uri(f"s3://my-bucket/first_dir/")
    new_file = first_dir / "some_dir" / "empty.txt"
    new_file.touch()
    print()
    print(f'Globing: {first_dir=}, pattern: "*"')
    assert list(first_dir.glob("*")) == [S3Path('/my-bucket/first_dir/some_dir/')]

    second_dir = S3Path.from_uri(f"s3://my-bucket/first_dir/second_dir/")
    new_file = second_dir / "some_dir" / "empty.txt"
    new_file.touch()
    print()
    print(f'Globing: {second_dir=}, pattern: "*"')
    assert list(second_dir.glob("*")) == [S3Path('/my-bucket/first_dir/second_dir/some_dir/')]

    third_dir = S3Path.from_uri(f"s3://my-bucket/first_dir/second_dir/third_dir/")
    new_file = third_dir / "some_dir" / "empty.txt"
    new_file.touch()
    print()
    print(f'Globing: {third_dir=}, pattern: "*"')
    assert list(third_dir.glob("*")) == [S3Path('/my-bucket/first_dir/second_dir/third_dir/some_dir/')]


def test_glob_nested_folders_issue_no_179(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='my-bucket')
    example_paths = [
        's3path/nested/further/andfurther/too_far_1.txt',
        's3path/nested/further/andfurther/too_far_2.txt',
    ]
    for example_path in example_paths:
        object_summary = s3.ObjectSummary('my-bucket', f'{example_path}/test.txt')
        object_summary.put(Body=b'test data')

    path = S3Path.from_uri("s3://my-bucket/s3path/nested")
    assert list(path.glob("*/*")) == [
        S3Path('/my-bucket/s3path/nested/further/andfurther')]


def test_rglob(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    assert list(S3Path('/test-bucket/').rglob('*.test')) == [S3Path('/test-bucket/directory/Test.test')]
    assert list(S3Path('/test-bucket/').rglob('**/*.test')) == [S3Path('/test-bucket/directory/Test.test')]

    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')

    assert sorted(S3Path.from_uri('s3://test-bucket/').rglob('*.py')) == [
        S3Path('/test-bucket/build/lib/pathlib.py'),
        S3Path('/test-bucket/docs/conf.py'),
        S3Path('/test-bucket/pathlib.py'),
        S3Path('/test-bucket/setup.py'),
        S3Path('/test-bucket/test_pathlib.py')]


def test_rglob_old_algo(s3_mock, enable_old_glob):
    if sys.version_info > (3, 12):
        with pytest.deprecated_call():
            test_rglob(s3_mock)
    else:
        test_rglob(s3_mock)


def test_accessor_scandir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')

    assert sorted(S3Path.from_uri('s3://test-bucket/').rglob('*.py')) == [
        S3Path('/test-bucket/build/lib/pathlib.py'),
        S3Path('/test-bucket/docs/conf.py'),
        S3Path('/test-bucket/pathlib.py'),
        S3Path('/test-bucket/setup.py'),
        S3Path('/test-bucket/test_pathlib.py')]


def test_accessor_scandir_old_algo(s3_mock, enable_old_glob):
    if sys.version_info > (3, 12):
        with pytest.deprecated_call():
            test_accessor_scandir(s3_mock)
    else:
        test_accessor_scandir(s3_mock)


def test_is_dir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')

    assert S3Path('/').is_dir()
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
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')

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


def test_read_line(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data\ntest data')

    with S3Path('/test-bucket/directory/Test.test').open("r") as fp:
        assert fp.readline() == "test data\n"
        assert fp.readline() == "test data"
        assert fp.readline() == ""


def test_read_lines(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data\ntest data')

    with S3Path('/test-bucket/directory/Test.test').open("r") as fp:
        assert len(fp.readlines()) == 2


def test_fix_url_encoding_issue(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'paramA=valueA/paramB=valueB/name')
    object_summary.put(Body=b'test data\ntest data')

    assert S3Path('/test-bucket/paramA=valueA/paramB=valueB/name').read_bytes() == b'test data\ntest data'


def test_read_lines_hint(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data\ntest data')

    with S3Path('/test-bucket/directory/Test.test').open() as fp:
        assert len(fp.readlines(1)) == (1 if sys.version_info >= (3, 6) else 2)

    with S3Path('/test-bucket/directory/Test.test').open('br') as fp:
        assert len(fp.readlines(1)) == 1  # work only in binary mode


def test_iter_lines(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data\ntest data\n')

    with S3Path('/test-bucket/directory/Test.test').open("r") as fp:
        for line in fp:
            assert line == "test data\n"


def test_write_lines(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')

    path = S3Path('/test-bucket/directory/Test.test')
    with path.open("w") as fp:
        fp.writelines(["line 1\n", "line 2\n"])

    res = path.read_text().splitlines()
    assert len(res) == 2


def test_iterdir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/make.bat')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/index.rst')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/Makefile')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_templates/11conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_build/22conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_static/conf.py')
    object_summary.put(Body=b'test data')

    s3_path = S3Path('/test-bucket/docs')
    assert sorted(s3_path.iterdir()) == sorted([
        S3Path('/test-bucket/docs/_build'),
        S3Path('/test-bucket/docs/_static'),
        S3Path('/test-bucket/docs/_templates'),
        S3Path('/test-bucket/docs/conf.py'),
        S3Path('/test-bucket/docs/index.rst'),
        S3Path('/test-bucket/docs/make.bat'),
        S3Path('/test-bucket/docs/Makefile'),
    ])


def test_iterdir_on_buckets(s3_mock):
    s3 = boto3.resource('s3')
    for index in range(4):
        s3.create_bucket(Bucket='test-bucket{}'.format(index))

    s3_root_path = S3Path('/')
    assert sorted(s3_root_path.iterdir()) == [
        S3Path('/test-bucket{}'.format(index))
        for index in range(4)
    ]


def test_empty_directory(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')

    assert list(S3Path('/test-bucket').iterdir()) == []

    s3.meta.client.put_object(Bucket='test-bucket', Key='to/empty/dir/')
    assert list(S3Path('/test-bucket/to/empty/dir/').iterdir()) == []


def test_open_for_reading(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    file_obj = path.open()
    assert file_obj.read() == 'test data'


def test_presigned_url(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    presigned_url = path.get_presigned_url()
    assert requests.get(presigned_url).content == b'test data'


def test_presigned_url_expire(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    presigned_url = path.get_presigned_url(expire_in=123)
    assert requests.get(presigned_url).content == b'test data'


def test_presigned_url_expire_with_timedelta(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    presigned_url = path.get_presigned_url(expire_in=timedelta(seconds=123))
    assert requests.get(presigned_url).content == b'test data'


def test_presigned_url_expire_with_negative_timedelta(s3_mock):
    path = S3Path('/test-bucket/directory/Test.test')
    with pytest.raises(ValueError) as err:
        path.get_presigned_url(expire_in=timedelta(seconds=-123))
    assert str(err.value) == (
        "The expire_in argument can't represent a negative or null time delta. "
        "You provided expire_in = -123 seconds which is below or equal to 0 seconds."
    )


def test_presigned_url_expire_with_negative_seconds(s3_mock):
    path = S3Path('/test-bucket/directory/Test.test')
    with pytest.raises(ValueError) as err:
        path.get_presigned_url(expire_in=-123)
    assert str(err.value) == (
        "The expire_in argument can't represent a negative or null time delta. "
        "You provided expire_in = -123 seconds which is below or equal to 0 seconds."
    )


def test_presigned_url_malformed_path(s3_mock):
    path = S3Path('Test.test')
    with pytest.raises(ValueError) as err:
        path.get_presigned_url(expire_in=timedelta(seconds=123))
    assert str(err.value) == "relative path have no bucket, key specification"


def test_open_for_write(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    bucket = s3.Bucket('test-bucket')
    assert sum(1 for _ in bucket.objects.all()) == 0

    path = S3Path('/test-bucket/directory/Test.test')

    with path.open(mode='bw') as file_obj:
        assert file_obj.writable()
        file_obj.write(b'test data\n')
        file_obj.writelines([b'test data'])
    assert sum(1 for _ in bucket.objects.all()) == 1

    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    streaming_body = object_summary.get()['Body']

    assert list(streaming_body.iter_lines()) == [
        b'test data',
        b'test data'
    ]


def test_open_binary_read(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    with path.open(mode='br') as file_obj:
        assert file_obj.readlines() == [b'test data']

    with path.open(mode='rb') as file_obj:
        assert file_obj.readline() == b'test data'
        assert file_obj.readline() == b''
        assert file_obj.readline() == b''


def test_read_bytes(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    assert path.read_bytes() == b'test data'


def test_open_text_read(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    with path.open(mode='r') as file_obj:
        assert file_obj.readlines() == ['test data']

    with path.open(mode='rt') as file_obj:
        assert file_obj.readline() == 'test data'
        assert file_obj.readline() == ''
        assert file_obj.readline() == ''


def test_read_text(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    assert path.read_text() == 'test data'


def test_owner(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    assert path.owner() == 'webfile'


def test_rename_s3_to_s3(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/make.bat')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/index.rst')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/Makefile')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_templates/11conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_build/22conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_static/conf.py')
    object_summary.put(Body=b'test data')

    s3.create_bucket(Bucket='target-bucket')

    S3Path('/test-bucket/docs/conf.py').rename('/test-bucket/docs/conf1.py')
    assert not S3Path('/test-bucket/docs/conf.py').exists()
    assert S3Path('/test-bucket/docs/conf1.py').is_file()

    path = S3Path('/test-bucket/docs/')
    path.rename(S3Path('/target-bucket') / S3Path('folder'))
    assert not path.exists()
    assert S3Path('/target-bucket/folder/conf1.py').is_file()
    assert S3Path('/target-bucket/folder/make.bat').is_file()
    assert S3Path('/target-bucket/folder/index.rst').is_file()
    assert S3Path('/target-bucket/folder/Makefile').is_file()
    assert S3Path('/target-bucket/folder/_templates/11conf.py').is_file()
    assert S3Path('/target-bucket/folder/_build/22conf.py').is_file()
    assert S3Path('/target-bucket/folder/_static/conf.py').is_file()


def test_replace_s3_to_s3(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/make.bat')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/index.rst')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/Makefile')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_templates/11conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_build/22conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_static/conf.py')
    object_summary.put(Body=b'test data')

    s3.create_bucket(Bucket='target-bucket')

    S3Path('/test-bucket/docs/conf.py').replace('/test-bucket/docs/conf1.py')
    assert not S3Path('/test-bucket/docs/conf.py').exists()
    assert S3Path('/test-bucket/docs/conf1.py').is_file()

    path = S3Path('/test-bucket/docs/')
    path.replace(S3Path('/target-bucket') / S3Path('folder'))
    assert not path.exists()
    assert S3Path('/target-bucket/folder/conf1.py').is_file()
    assert S3Path('/target-bucket/folder/make.bat').is_file()
    assert S3Path('/target-bucket/folder/index.rst').is_file()
    assert S3Path('/target-bucket/folder/Makefile').is_file()
    assert S3Path('/target-bucket/folder/_templates/11conf.py').is_file()
    assert S3Path('/target-bucket/folder/_build/22conf.py').is_file()
    assert S3Path('/target-bucket/folder/_static/conf.py').is_file()


def test_rmdir(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/make.bat')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/index.rst')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/Makefile')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_templates/11conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_build/22conf.py')
    object_summary.put(Body=b'test data')
    object_summary = s3.ObjectSummary('test-bucket', 'docs/_static/conf.py')
    object_summary.put(Body=b'test data')

    conf_path = S3Path('/test-bucket/docs/_templates')
    assert conf_path.is_dir()
    conf_path.rmdir()
    assert not conf_path.exists()

    path = S3Path('/test-bucket/docs/')
    path.rmdir()
    assert not path.exists()


def test_rmdir_can_remove_bucket(s3_mock):
    s3 = boto3.resource('s3')
    bucket = S3Path('/test-bucket/')
    bucket.mkdir()
    assert bucket.exists()
    bucket.rmdir()
    assert not bucket.exists()


def test_mkdir(s3_mock):
    s3 = boto3.resource('s3')

    S3Path('/test-bucket/').mkdir()

    assert s3.Bucket('test-bucket') in s3.buckets.all()

    S3Path('/test-bucket/').mkdir(exist_ok=True)

    with pytest.raises(FileExistsError):
        S3Path('/test-bucket/').mkdir(exist_ok=False)

    with pytest.raises(FileNotFoundError):
        S3Path('/test-second-bucket/test-directory/file.name').mkdir()

    S3Path('/test-second-bucket/test-directory/file.name').mkdir(parents=True)

    assert s3.Bucket('test-second-bucket') in s3.buckets.all()


def test_write_text(s3_mock):
    s3 = boto3.resource('s3')

    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'temp_key')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/temp_key')
    data = path.read_text()
    assert isinstance(data, str)

    path.write_text(data)
    assert path.read_text() == data


def test_write_bytes(s3_mock):
    s3 = boto3.resource('s3')

    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'temp_key')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/temp_key')
    data = path.read_bytes()
    assert isinstance(data, bytes)

    path.write_bytes(data)
    assert path.read_bytes() == data


def test_unlink(s3_mock):
    s3 = boto3.resource('s3')

    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'temp_key')
    object_summary.put(Body=b'test data')
    path = S3Path('/test-bucket/temp_key')
    subdir_key = S3Path('/test-bucket/fake_folder/some_key')
    subdir_key.write_text("some text")
    assert path.exists() is True
    assert subdir_key.exists() is True
    path.unlink()
    assert path.exists() is False
    with pytest.raises(FileNotFoundError):
        S3Path("/test-bucket/fake_subfolder/fake_subkey").unlink()
    with pytest.raises(IsADirectoryError):
        S3Path("/test-bucket/fake_folder").unlink()
    with pytest.raises(IsADirectoryError):
        S3Path("/fake-bucket/").unlink()

    S3Path("/test-bucket/fake_subfolder/fake_subkey").unlink(missing_ok=True)
    S3Path("/test-bucket/fake_folder").unlink(missing_ok=True)
    S3Path("/fake-bucket/").unlink(missing_ok=True)


def test_absolute(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    absolute_path = S3Path('/test-bucket/directory/Test.test')
    assert absolute_path.absolute() is absolute_path

    relative_path = S3Path('./Test.test')
    with pytest.raises(ValueError):
        relative_path.absolute()


def test_versioned_bucket(s3_mock):
    bucket, key = 'test-versioned-bucket', 'versioned_file.txt'

    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket)
    s3.BucketVersioning(bucket).enable()

    object_summary = s3.ObjectSummary(bucket, key)
    file_contents_by_version = (b'Test', b'Test updated', b'Test', b'Test final')

    version_id_to_file_content = {}
    for file_content in file_contents_by_version:
        version_id = object_summary.put(Body=file_content).get('VersionId')
        version_id_to_file_content[version_id] = file_content

    assert len(version_id_to_file_content) == len(file_contents_by_version)

    # Test that we can read specific versions of the file
    for version_id, expected_file_content in version_id_to_file_content.items():
        versioned_paths = (
            VersionedS3Path(f'/{bucket}/{key}', version_id=version_id),
            VersionedS3Path(f'/{bucket}', f'{key}', version_id=version_id),
            VersionedS3Path.from_uri(f's3://{bucket}/{key}', version_id=version_id),
            VersionedS3Path.from_bucket_key(bucket=bucket, key=key, version_id=version_id),
        )
        for versioned_path in versioned_paths:
            assert versioned_path.exists() and versioned_path.is_file()
            assert versioned_path.stat().st_version_id == version_id
            assert versioned_path.read_bytes() == expected_file_content

    # Test that we receive the latest version of the file when S3Path is used or no version_id is specified
    paths = (
        S3Path(f'/{bucket}/{key}'),
        S3Path(f'/{bucket}', f'{key}'),
        S3Path.from_uri(f's3://{bucket}/{key}'),
        S3Path.from_bucket_key(bucket=bucket, key=key),
    )
    for path in paths:
        assert not isinstance(path, VersionedS3Path)
        assert path.read_bytes() == file_contents_by_version[-1]


def test_buffered_copy(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    data = b'test data' * 10_000_000
    source_path = S3Path('/test-bucket/source')
    source_path.write_bytes(data)
    target_path = S3Path('/test-bucket/target')
    with source_path.open('rb') as source, target_path.open('wb') as target:
        shutil.copyfileobj(source, target)
    assert target_path.read_bytes() == data
