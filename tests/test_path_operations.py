import sys
from pathlib import Path

import mock
import pytest
from google.cloud import storage

from gcspath import GCSPath, PureGCSPath, StatResult, _gcs_accessor

# todo: test samefile/touch/write_text/write_bytes method
# todo: test security and boto config changes
# todo: test open method check R/W bytes/unicode
# todo: test adding parameners to boto3 by path


@pytest.fixture()
def gcs_mock():
    client = mock.create_autospec(storage.Client)
    yield client


def test_path_support():
    assert PureGCSPath in GCSPath.mro()
    assert Path in GCSPath.mro()


def test_stat(gcs_mock):
    path = GCSPath("fake-bucket-1234-0987/fake-key")
    with pytest.raises(ValueError):
        path.stat()
    test_file = "gs://gcsbucket-test-dev/foo.txt"
    client = storage.Client()
    blob = storage.Blob.from_string(test_file)
    blob.upload_from_string("New contents!", client=client)
    path = GCSPath.from_uri(test_file)
    stat = path.stat()
    assert isinstance(stat, StatResult)
    assert stat == StatResult(size=blob.size, last_modified=blob.updated)
    path = GCSPath("/test-bucket")
    assert path.stat() is None


def test_exists(gcs_mock):
    path = GCSPath("./fake-key")
    with pytest.raises(ValueError):
        path.exists()

    # GCS buckets are globally unique and I don't have access to "test-bucket"
    # I guess if you own that bucket and execute these tests, they might fail. |x_X|
    assert GCSPath("/test-bucket/fake-key").exists() is False
    assert GCSPath("/test-bucket-3415155/fake-key").exists() is False
    assert GCSPath("/gcsbucket-test-dev/not_found.txt").exists() is False

    test_path = "/gcsbucket-test-dev/directory/foo.txt"
    test_gs_file = f"gs:/{test_path}"
    client = storage.Client()
    blob = storage.Blob.from_string(test_gs_file)
    blob.upload_from_string("New contents!", client=client)
    path = GCSPath(test_path)
    assert path.exists()
    for parent in path.parents:
        assert parent.exists()


# def test_glob(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     assert list(GCSPath("/test-bucket/").glob("*.test")) == []
#     assert list(GCSPath("/test-bucket/directory/").glob("*.test")) == [
#         GCSPath("/test-bucket/directory/Test.test")
#     ]
#     assert list(GCSPath("/test-bucket/").glob("**/*.test")) == [
#         GCSPath("/test-bucket/directory/Test.test")
#     ]

#     object_summary = s3.ObjectSummary("test-bucket", "pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "setup.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "test_pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "build/lib/pathlib.py")
#     object_summary.put(Body=b"test data")

#     assert sorted(GCSPath.from_uri("s3://test-bucket/").glob("*.py")) == [
#         GCSPath("/test-bucket/pathlib.py"),
#         GCSPath("/test-bucket/setup.py"),
#         GCSPath("/test-bucket/test_pathlib.py"),
#     ]
#     assert sorted(GCSPath.from_uri("s3://test-bucket/").glob("*/*.py")) == [
#         GCSPath("/test-bucket/docs/conf.py")
#     ]
#     assert sorted(GCSPath.from_uri("s3://test-bucket/").glob("**/*.py")) == [
#         GCSPath("/test-bucket/build/lib/pathlib.py"),
#         GCSPath("/test-bucket/docs/conf.py"),
#         GCSPath("/test-bucket/pathlib.py"),
#         GCSPath("/test-bucket/setup.py"),
#         GCSPath("/test-bucket/test_pathlib.py"),
#     ]
#     assert sorted(GCSPath.from_uri("s3://test-bucket/").glob("*cs")) == [
#         GCSPath("/test-bucket/docs/"),
#     ]


# def test_rglob(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     assert list(GCSPath("/test-bucket/").rglob("*.test")) == [
#         GCSPath("/test-bucket/directory/Test.test")
#     ]
#     assert list(GCSPath("/test-bucket/").rglob("**/*.test")) == [
#         GCSPath("/test-bucket/directory/Test.test")
#     ]

#     object_summary = s3.ObjectSummary("test-bucket", "pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "setup.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "test_pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "build/lib/pathlib.py")
#     object_summary.put(Body=b"test data")

#     assert sorted(GCSPath.from_uri("s3://test-bucket/").rglob("*.py")) == [
#         GCSPath("/test-bucket/build/lib/pathlib.py"),
#         GCSPath("/test-bucket/docs/conf.py"),
#         GCSPath("/test-bucket/pathlib.py"),
#         GCSPath("/test-bucket/setup.py"),
#         GCSPath("/test-bucket/test_pathlib.py"),
#     ]


# def test_is_dir(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "setup.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "test_pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "build/lib/pathlib.py")
#     object_summary.put(Body=b"test data")

#     assert not GCSPath("/test-bucket/fake.test").is_dir()
#     assert not GCSPath("/test-bucket/fake/").is_dir()
#     assert GCSPath("/test-bucket/directory").is_dir()
#     assert not GCSPath("/test-bucket/directory/Test.test").is_dir()
#     assert not GCSPath("/test-bucket/pathlib.py").is_dir()
#     assert not GCSPath("/test-bucket/docs/conf.py").is_dir()
#     assert GCSPath("/test-bucket/docs/").is_dir()
#     assert GCSPath("/test-bucket/build/").is_dir()
#     assert GCSPath("/test-bucket/build/lib").is_dir()
#     assert not GCSPath("/test-bucket/build/lib/pathlib.py").is_dir()


# def test_is_file(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "setup.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "test_pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "build/lib/pathlib.py")
#     object_summary.put(Body=b"test data")

#     assert not GCSPath("/test-bucket/fake.test").is_file()
#     assert not GCSPath("/test-bucket/fake/").is_file()
#     assert not GCSPath("/test-bucket/directory").is_file()
#     assert GCSPath("/test-bucket/directory/Test.test").is_file()
#     assert GCSPath("/test-bucket/pathlib.py").is_file()
#     assert GCSPath("/test-bucket/docs/conf.py").is_file()
#     assert not GCSPath("/test-bucket/docs/").is_file()
#     assert not GCSPath("/test-bucket/build/").is_file()
#     assert not GCSPath("/test-bucket/build/lib").is_file()
#     assert GCSPath("/test-bucket/build/lib/pathlib.py").is_file()


# def test_iterdir(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "setup.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "test_pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "build/lib/pathlib.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/make.bat")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/index.rst")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/Makefile")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_templates/11conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_build/22conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_static/conf.py")
#     object_summary.put(Body=b"test data")

#     s3_path = GCSPath("/test-bucket/docs")
#     assert sorted(s3_path.iterdir()) == [
#         GCSPath("/test-bucket/docs/Makefile"),
#         GCSPath("/test-bucket/docs/_build"),
#         GCSPath("/test-bucket/docs/_static"),
#         GCSPath("/test-bucket/docs/_templates"),
#         GCSPath("/test-bucket/docs/conf.py"),
#         GCSPath("/test-bucket/docs/index.rst"),
#         GCSPath("/test-bucket/docs/make.bat"),
#     ]


# def test_open_for_reading(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     file_obj = path.open()
#     assert file_obj.read() == "test data"


# def test_open_for_write(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     bucket = s3.Bucket("test-bucket")
#     assert sum(1 for _ in bucket.objects.all()) == 0

#     path = GCSPath("/test-bucket/directory/Test.test")
#     file_obj = path.open(mode="bw")
#     assert file_obj.writable()
#     file_obj.write(b"test data\n")
#     file_obj.writelines([b"test data"])

#     assert sum(1 for _ in bucket.objects.all()) == 1

#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     streaming_body = object_summary.get()["Body"]

#     assert list(streaming_body.iter_lines()) == [b"test data", b"test data"]


# def test_open_binary_read(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     with path.open(mode="br") as file_obj:
#         assert file_obj.readlines() == [b"test data"]

#     with path.open(mode="rb") as file_obj:
#         assert file_obj.readline() == b"test data"
#         assert file_obj.readline() == b""
#         assert file_obj.readline() == b""


# @pytest.mark.skipif(sys.version_info < (3, 5), reason="requires python3.5 or higher")
# def test_read_bytes(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     assert path.read_bytes() == b"test data"


# def test_open_text_read(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     with path.open(mode="r") as file_obj:
#         assert file_obj.readlines() == ["test data"]

#     with path.open(mode="rt") as file_obj:
#         assert file_obj.readline() == "test data"
#         assert file_obj.readline() == ""
#         assert file_obj.readline() == ""


# @pytest.mark.skipif(sys.version_info < (3, 5), reason="requires python3.5 or higher")
# def test_read_text(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     assert path.read_text() == "test data"


# def test_owner(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "directory/Test.test")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/directory/Test.test")
#     assert path.owner() == "webfile"


# def test_rename_s3_to_s3(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/make.bat")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/index.rst")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/Makefile")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_templates/11conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_build/22conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_static/conf.py")
#     object_summary.put(Body=b"test data")

#     s3.create_bucket(Bucket="target-bucket")

#     GCSPath("/test-bucket/docs/conf.py").rename("/test-bucket/docs/conf1.py")
#     assert not GCSPath("/test-bucket/docs/conf.py").exists()
#     assert GCSPath("/test-bucket/docs/conf1.py").is_file()

#     path = GCSPath("/test-bucket/docs/")
#     path.rename(GCSPath("/target-bucket") / GCSPath("folder"))
#     assert not path.exists()
#     assert GCSPath("/target-bucket/folder/conf1.py").is_file()
#     assert GCSPath("/target-bucket/folder/make.bat").is_file()
#     assert GCSPath("/target-bucket/folder/index.rst").is_file()
#     assert GCSPath("/target-bucket/folder/Makefile").is_file()
#     assert GCSPath("/target-bucket/folder/_templates/11conf.py").is_file()
#     assert GCSPath("/target-bucket/folder/_build/22conf.py").is_file()
#     assert GCSPath("/target-bucket/folder/_static/conf.py").is_file()


# def test_replace_s3_to_s3(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/make.bat")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/index.rst")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/Makefile")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_templates/11conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_build/22conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_static/conf.py")
#     object_summary.put(Body=b"test data")

#     s3.create_bucket(Bucket="target-bucket")

#     GCSPath("/test-bucket/docs/conf.py").replace("/test-bucket/docs/conf1.py")
#     assert not GCSPath("/test-bucket/docs/conf.py").exists()
#     assert GCSPath("/test-bucket/docs/conf1.py").is_file()

#     path = GCSPath("/test-bucket/docs/")
#     path.replace(GCSPath("/target-bucket") / GCSPath("folder"))
#     assert not path.exists()
#     assert GCSPath("/target-bucket/folder/conf1.py").is_file()
#     assert GCSPath("/target-bucket/folder/make.bat").is_file()
#     assert GCSPath("/target-bucket/folder/index.rst").is_file()
#     assert GCSPath("/target-bucket/folder/Makefile").is_file()
#     assert GCSPath("/target-bucket/folder/_templates/11conf.py").is_file()
#     assert GCSPath("/target-bucket/folder/_build/22conf.py").is_file()
#     assert GCSPath("/target-bucket/folder/_static/conf.py").is_file()


# def test_rmdir(gcs_mock):
#     s3 = boto3.resource("s3")
#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/make.bat")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/index.rst")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/Makefile")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_templates/11conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_build/22conf.py")
#     object_summary.put(Body=b"test data")
#     object_summary = s3.ObjectSummary("test-bucket", "docs/_static/conf.py")
#     object_summary.put(Body=b"test data")

#     conf_path = GCSPath("/test-bucket/docs/_templates")
#     assert conf_path.is_dir()
#     conf_path.rmdir()
#     assert not conf_path.exists()

#     path = GCSPath("/test-bucket/docs/")
#     path.rmdir()
#     assert not path.exists()


# def test_mkdir(gcs_mock):
#     s3 = boto3.resource("s3")

#     GCSPath("/test-bucket/").mkdir()

#     assert s3.Bucket("test-bucket") in s3.buckets.all()

#     GCSPath("/test-bucket/").mkdir(exist_ok=True)

#     with pytest.raises(FileExistsError):
#         GCSPath("/test-bucket/").mkdir(exist_ok=False)

#     with pytest.raises(FileNotFoundError):
#         GCSPath("/test-second-bucket/test-directory/file.name").mkdir()

#     GCSPath("/test-second-bucket/test-directory/file.name").mkdir(parents=True)

#     assert s3.Bucket("test-second-bucket") in s3.buckets.all()


# def test_write_text(gcs_mock):
#     s3 = boto3.resource("s3")

#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "temp_key")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/temp_key")
#     data = path.read_text()
#     assert isinstance(data, str)

#     path.write_text(data)
#     assert path.read_text() == data


# def test_write_bytes(gcs_mock):
#     s3 = boto3.resource("s3")

#     s3.create_bucket(Bucket="test-bucket")
#     object_summary = s3.ObjectSummary("test-bucket", "temp_key")
#     object_summary.put(Body=b"test data")

#     path = GCSPath("/test-bucket/temp_key")
#     data = path.read_bytes()
#     assert isinstance(data, bytes)

#     path.write_bytes(data)
#     assert path.read_bytes() == data
