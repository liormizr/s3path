import errno
import gc

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from s3path import S3Path, VersionedS3Path, register_configuration_parameter


@pytest.fixture()
def destination(s3_mock):
    resource = boto3.resource('s3', region_name='us-east-1')
    resource.create_bucket(Bucket='test-bucket')
    path = S3Path('/test-bucket/object')
    register_configuration_parameter(path.parent, resource=resource)
    return path, resource.meta.client


@pytest.mark.parametrize('mode', ['x', 'xt', 'tx', 'xb', 'bx'])
@pytest.mark.parametrize('empty', [False, True])
def test_exclusive_creation(destination, mode, empty):
    path, client = destination
    binary = 'b' in mode
    data = '' if empty else 'café\n'
    options = {} if binary else {'encoding': 'utf-8', 'newline': '\r\n'}
    with path.open(mode, **options) as stream:
        stream.write(data.encode() if binary else data)
        # Opening or writing must not reserve the key with a placeholder.
        assert not path.exists()
    expected = data if binary else data.replace('\n', '\r\n')
    assert path.read_bytes() == expected.encode()
    assert stream.closed
    stream.close()
    with pytest.raises(ValueError):
        stream.write(b'later' if binary else 'later')
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


@pytest.mark.parametrize('mode', ['x', 'xb'])
@pytest.mark.parametrize('empty', [False, True])
def test_existing_object_is_unchanged(destination, mode, empty):
    path, client = destination
    path.write_bytes(b'original')
    with pytest.raises(FileExistsError) as raised:
        with path.open(mode) as stream:
            if not empty:
                stream.write(b'replacement' if mode == 'xb' else 'replacement')
    assert raised.value.errno == errno.EEXIST
    assert raised.value.filename == str(path)
    assert isinstance(raised.value.__cause__, ClientError)
    assert raised.value.__cause__.operation_name == ('PutObject' if empty else 'CompleteMultipartUpload')
    assert path.read_bytes() == b'original'
    assert stream.closed
    stream.close()
    del raised
    del stream
    gc.collect()
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


@pytest.mark.parametrize('empty_winner', [False, True])
@pytest.mark.parametrize('empty_loser', [False, True])
def test_first_completion_wins(destination, empty_winner, empty_loser):
    path, client = destination
    loser = path.open('xb')
    winner = path.open('xb')
    loser.write(b'' if empty_loser else b'opened first, finished last')
    winner.write(b'' if empty_winner else b'winner')
    assert not path.exists()
    winner.close()
    assert len(client.list_multipart_uploads(Bucket=path.bucket)['Uploads']) == 1
    with pytest.raises(FileExistsError):
        loser.close()
    loser.close()
    assert path.read_bytes() == (b'' if empty_winner else b'winner')
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


def test_multiple_parts_are_published_conditionally(destination):
    path, client = destination
    # Cross smart_open's default 50 MiB part boundary without changing its internals.
    first_part = b'a' * (50 * 1024 * 1024)
    with path.open('xb') as stream:
        stream.write(first_part)
        uploads = client.list_multipart_uploads(Bucket=path.bucket)['Uploads']
        assert len(uploads) == 1
        parts = client.list_parts(Bucket=path.bucket, Key=path.key, UploadId=uploads[0]['UploadId'])
        assert len(parts['Parts']) == 1
        assert not path.exists()
        stream.write(b'last part')
    assert path.read_bytes() == first_part + b'last part'


def test_upload_failure_during_write_aborts(destination):
    path, client = destination
    stream = path.open('xb')
    with Stubber(client) as stubber:
        stubber.add_client_error('upload_part', service_error_code='AccessDenied', http_status_code=403)
        stubber.add_response('abort_multipart_upload', {})
        with pytest.raises(ClientError) as raised:
            stream.write(b'a' * (50 * 1024 * 1024))
        assert raised.value.operation_name == 'UploadPart'
        assert stream.closed
        stream.close()
        del stream
        gc.collect()
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('delete_marker', [False, True])
def test_versioned_bucket(destination, empty, delete_marker):
    path, client = destination
    client.put_bucket_versioning(Bucket=path.bucket, VersioningConfiguration={'Status': 'Enabled'})
    original = client.put_object(Bucket=path.bucket, Key=path.key, Body=b'original')['VersionId']
    if delete_marker:
        marker = client.delete_object(Bucket=path.bucket, Key=path.key)['VersionId']
    before = client.list_object_versions(Bucket=path.bucket)

    def create():
        with path.open('xb') as stream:
            stream.write(b'' if empty else b'new version')

    if delete_marker:
        create()
        assert path.read_bytes() == (b'' if empty else b'new version')
        after = client.list_object_versions(Bucket=path.bucket)
        assert len(after['Versions']) == 2
        assert after['DeleteMarkers'][0]['VersionId'] == marker
    else:
        with pytest.raises(FileExistsError):
            create()
        assert client.list_object_versions(Bucket=path.bucket)['Versions'] == before['Versions']
    assert VersionedS3Path(str(path), version_id=original).read_bytes() == b'original'
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


@pytest.mark.parametrize('mode', ['x', 'xb'])
def test_versioned_path_cannot_write(destination, mode):
    path, client = destination
    versioned = VersionedS3Path(str(path), version_id='historical-version')
    with Stubber(client):
        with pytest.raises(ValueError, match='version_id must be None'):
            versioned.open(mode)


@pytest.mark.parametrize('mode', ['x', 'xb'])
def test_body_exception_aborts_only_own_upload(destination, mode):
    path, client = destination
    other = path.open('xb')
    original_error = RuntimeError('body failed')
    with pytest.raises(RuntimeError) as raised:
        with path.open(mode) as stream:
            stream.write(b'partial' if mode == 'xb' else 'partial')
            raise original_error
    assert raised.value is original_error
    assert stream.closed
    stream.close()
    assert not path.exists()
    assert len(client.list_multipart_uploads(Bucket=path.bucket)['Uploads']) == 1
    other.write(b'other writer')
    other.close()
    assert path.read_bytes() == b'other writer'


@pytest.mark.parametrize('mode, options, data, error_type', [
    ('xb', {}, 'not bytes', TypeError),
    ('x', {'encoding': 'ascii'}, 'café', UnicodeEncodeError),
])
def test_write_error_aborts_without_context_manager(destination, mode, options, data, error_type):
    path, client = destination
    stream = path.open(mode, **options)
    with pytest.raises(error_type):
        stream.write(data)
    assert stream.closed
    stream.close()
    assert not path.exists()
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


@pytest.mark.parametrize('operation, code, status', [
    ('complete_multipart_upload', 'AccessDenied', 403),
    ('complete_multipart_upload', 'ConditionalRequestConflict', 409),
    ('complete_multipart_upload', 'NoSuchUpload', 404),
    ('upload_part', 'PreconditionFailed', 412),
    ('put_object', 'AccessDenied', 403),
    ('put_object', 'ConditionalRequestConflict', 409),
    ('put_object', 'NoSuchBucket', 404),
])
def test_other_errors_are_preserved(destination, monkeypatch, operation, code, status):
    path, client = destination
    stream = path.open('xb')
    if operation != 'put_object':
        stream.write(b'data')
    operation_name = client.meta.method_to_api_mapping[operation]
    original = ClientError({'Error': {'Code': code}, 'ResponseMetadata': {'HTTPStatusCode': status}}, operation_name)
    original.__cause__ = RuntimeError('original cause')

    def fail(**kwargs):
        raise original

    monkeypatch.setattr(client, operation, fail)
    with pytest.raises(ClientError) as raised:
        stream.close()
    assert raised.value is original
    assert str(raised.value.__cause__) == 'original cause'
    assert stream.closed
    stream.close()
    assert not path.exists()
    assert not client.list_multipart_uploads(Bucket=path.bucket).get('Uploads')


@pytest.mark.parametrize('empty', [False, True])
def test_conditional_requests_and_custom_configuration(destination, empty):
    path, client = destination
    parameters = {'Metadata': {'custom': 'value'}, 'ServerSideEncryption': 'AES256',
                  'RequestPayer': 'requester', 'IfNoneMatch': 'must-be-overridden',
                  'IfMatch': 'read-condition'}
    register_configuration_parameter(path.parent, parameters=parameters)
    requests = []

    def record(params, model, **kwargs):
        requests.append((model.name, params.copy()))

    client.meta.events.register('before-parameter-build.s3', record)
    with path.open('xb') as stream:
        stream.write(b'' if empty else b'data')
    operations = [name for name, params in requests]
    assert not set(operations) & {'HeadObject', 'ListObjects', 'ListObjectsV2', 'DeleteObject'}
    publish = 'PutObject' if empty else 'CompleteMultipartUpload'
    assert operations.count(publish) == 1
    for name, params in requests:
        if name == publish:
            assert params['IfNoneMatch'] == '*'
            assert 'IfMatch' not in params
            assert params['RequestPayer'] == 'requester'
        if name in ('CreateMultipartUpload', 'PutObject'):
            assert params['Metadata'] == {'custom': 'value'}
            assert params['ServerSideEncryption'] == 'AES256'
    assert parameters['IfNoneMatch'] == 'must-be-overridden'
    assert client.head_object(Bucket=path.bucket, Key=path.key)['Metadata'] == {'custom': 'value'}


def test_empty_put_failure_does_not_abort_twice(destination):
    path, client = destination
    stream = path.open('xb')
    upload_id = client.list_multipart_uploads(Bucket=path.bucket)['Uploads'][0]['UploadId']
    with Stubber(client) as stubber:
        stubber.add_response('abort_multipart_upload', {},
                             {'Bucket': path.bucket, 'Key': path.key, 'UploadId': upload_id})
        stubber.add_client_error('put_object', service_error_code='PreconditionFailed', http_status_code=412,
                                expected_params={'Bucket': path.bucket, 'Key': path.key, 'Body': b'', 'IfNoneMatch': '*'})
        with pytest.raises(FileExistsError):
            stream.close()
        assert stream.closed
        stream.close()
        stream.terminate()
        del stream
        gc.collect()
        stubber.assert_no_pending_responses()


def test_abort_failure_preserves_conflict_and_prevents_finalization_requests(destination):
    path, client = destination
    stream = path.open('xb')
    stream.write(b'data')
    with Stubber(client) as stubber:
        stubber.add_response('upload_part', {'ETag': 'etag'})
        stubber.add_client_error('complete_multipart_upload', service_error_code='PreconditionFailed',
                                http_status_code=412, expected_params={
                                    'Bucket': path.bucket, 'Key': path.key, 'UploadId': ANY,
                                    'MultipartUpload': ANY, 'IfNoneMatch': '*'})
        stubber.add_client_error('abort_multipart_upload', service_error_code='AccessDenied', http_status_code=403)
        with pytest.raises(FileExistsError) as raised:
            stream.close()
        assert raised.value.__cause__.operation_name == 'CompleteMultipartUpload'
        assert raised.value.__cause__.__context__.operation_name == 'AbortMultipartUpload'
        assert stream.closed
        stream.close()
        del raised
        del stream
        gc.collect()
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize('mode', ['x', 'xb'])
def test_abort_failure_does_not_mask_body_exception(destination, mode):
    path, client = destination
    original = RuntimeError('body failed')
    original.__cause__ = ValueError('original cause')
    stream = path.open(mode)
    with Stubber(client) as stubber:
        stubber.add_client_error('abort_multipart_upload', service_error_code='AccessDenied', http_status_code=403)
        with pytest.raises(RuntimeError) as raised:
            with stream:
                raise original
        assert raised.value is original
        assert raised.value.__context__.operation_name == 'AbortMultipartUpload'
        assert str(raised.value.__cause__) == 'original cause'
        stream.close()
        del raised
        del original
        del stream
        gc.collect()
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize('mode, options, error_type', [
    ('xx', {}, ValueError), ('xw', {}, ValueError), ('xr', {}, ValueError),
    ('xbt', {}, ValueError), ('x+', {}, NotImplementedError), ('xb+', {}, NotImplementedError),
    ('xb', {'encoding': 'utf-8'}, ValueError), ('xb', {'errors': 'strict'}, ValueError),
    ('xb', {'newline': '\n'}, ValueError), ('x', {'encoding': 'unknown-encoding'}, LookupError),
    ('x', {'newline': 'invalid'}, ValueError), ('x', {'buffering': 0}, ValueError),
])
def test_invalid_options_do_not_start_upload(destination, mode, options, error_type):
    path, client = destination
    with Stubber(client):
        with pytest.raises(error_type):
            path.open(mode, **options)


@pytest.mark.parametrize('mode, data', [('w', 'replacement'), ('wb', b'replacement'), ('wb', b'')])
def test_ordinary_write_still_replaces(destination, mode, data):
    path, client = destination
    path.write_bytes(b'original')
    with path.open(mode) as stream:
        stream.write(data)
    assert path.read_bytes() == (data.encode() if isinstance(data, str) else data)
