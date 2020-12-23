
import boto3
import smart_open

from s3path import PureS3Path, S3Path, StatResult


def test_glob(s3_mock):
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket='test-bucket')
    object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
    object_summary.put(Body=b'test data')

    path = S3Path('/test-bucket/directory/Test.test')
    uri = path.as_uri()
    import ipdb; ipdb.set_trace()
    smart_open.open(uri)
    pass




# def test_glob(s3_mock):
#     s3 = boto3.resource('s3')
#     s3.create_bucket(Bucket='test-bucket')
#     object_summary = s3.ObjectSummary('test-bucket', 'directory/Test.test')
#     object_summary.put(Body=b'test data')
#
#     assert list(S3Path('/test-bucket/').glob('*.test')) == []
#     assert list(S3Path('/test-bucket/directory/').glob('*.test')) == [S3Path('/test-bucket/directory/Test.test')]
#     assert list(S3Path('/test-bucket/').glob('**/*.test')) == [S3Path('/test-bucket/directory/Test.test')]
#
#     object_summary = s3.ObjectSummary('test-bucket', 'pathlib.py')
#     object_summary.put(Body=b'test data')
#     object_summary = s3.ObjectSummary('test-bucket', 'setup.py')
#     object_summary.put(Body=b'test data')
#     object_summary = s3.ObjectSummary('test-bucket', 'test_pathlib.py')
#     object_summary.put(Body=b'test data')
#     object_summary = s3.ObjectSummary('test-bucket', 'docs/conf.py')
#     object_summary.put(Body=b'test data')
#     object_summary = s3.ObjectSummary('test-bucket', 'build/lib/pathlib.py')
#     object_summary.put(Body=b'test data')
#
#     assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*.py')) == [
#         S3Path('/test-bucket/pathlib.py'),
#         S3Path('/test-bucket/setup.py'),
#         S3Path('/test-bucket/test_pathlib.py')]
#     assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*/*.py')) == [S3Path('/test-bucket/docs/conf.py')]
#     assert sorted(S3Path.from_uri('s3://test-bucket/').glob('**/*.py')) == [
#         S3Path('/test-bucket/build/lib/pathlib.py'),
#         S3Path('/test-bucket/docs/conf.py'),
#         S3Path('/test-bucket/pathlib.py'),
#         S3Path('/test-bucket/setup.py'),
#         S3Path('/test-bucket/test_pathlib.py')]
#     assert sorted(S3Path.from_uri('s3://test-bucket/').glob('*cs')) == [
#         S3Path('/test-bucket/docs/'),
#     ]
