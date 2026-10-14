import errno
from io import BytesIO, TextIOWrapper

from botocore.exceptions import ClientError
from smart_open.s3 import MultipartWriter


def open(path, *, mode, client, client_kwargs, buffering, encoding, errors, newline):
    if (set(mode) - set('xtb+') or len(set(mode)) != len(mode)
            or 'b' in mode and 't' in mode):
        raise ValueError(f'invalid mode: {mode!r}')
    if '+' in mode:
        raise NotImplementedError('S3 does not support updating an open object')
    if getattr(path, 'version_id', None) is not None:
        raise ValueError('version_id must be None when writing')
    binary = 'b' in mode
    if binary:
        if encoding is not None or errors is not None or newline is not None:
            raise ValueError('binary mode does not take encoding, errors or newline arguments')
    else:
        if buffering == 0:
            raise ValueError("can't have unbuffered text I/O")
        # Validate text options before creating a multipart upload.
        TextIOWrapper(BytesIO(), encoding=encoding, errors=errors, newline=newline).close()

    writer = _ExclusiveWriter(path, client=client, client_kwargs=client_kwargs)
    if binary:
        return writer
    try:
        return _ExclusiveTextWriter(
            writer, encoding=encoding, errors=errors, newline=newline, write_through=True)
    except BaseException as error:
        writer._abort_on_error(error)
        raise


class _ExclusiveWriter(MultipartWriter):
    def __init__(self, path, *, client, client_kwargs):
        self._path = str(path)
        super().__init__(path.bucket, path.key, client=client, client_kwargs=client_kwargs)
        self.name = path.key
        self.mode = 'xb'

    def close(self):
        if self.closed:
            return
        try:
            try:
                if self.tell() == 0:
                    # smart_open (including 7.3.0.post1) aborts empty uploads, then
                    # calls PutObject before clearing _upload_id. A failed PUT
                    # leaves it open and finalization can abort the same upload
                    # again. Terminate first so the empty PUT cannot leave stale
                    # upload state. Both publication paths remain conditional.
                    self.terminate()
                    self._client.put_object(Bucket=self._bucket, Key=self._key, Body=b'')
                else:
                    if self._buf.tell():
                        self._upload_next_part()
                    # Let botocore handle request retries. In particular, do not
                    # retry NoSuchUpload through smart_open's completion loop,
                    # which replaces the S3 error after exhausting its retries.
                    self._client.complete_multipart_upload(
                        Bucket=self._bucket, Key=self._key, UploadId=self._upload_id,
                        MultipartUpload={'Parts': self._parts})
                    self._upload_id = None
            except BaseException as error:
                self._abort_on_error(error)
                raise
        except ClientError as error:
            if (error.operation_name in ('PutObject', 'CompleteMultipartUpload')
                    and error.response.get('Error', {}).get('Code') in ('PreconditionFailed', '412')):
                raise FileExistsError(errno.EEXIST, 'File exists', self._path) from error
            raise

    def write(self, data):
        if self.closed:
            raise ValueError('write to closed file')
        try:
            return super().write(data)
        except BaseException as error:
            self._abort_on_error(error)
            raise

    def terminate(self):
        if self.closed:
            return
        try:
            super().terminate()
        finally:
            # Even if abort fails, close/finalization must never publish the
            # failed write. The abort error is still raised to the caller.
            self._upload_id = None

    def _abort_on_error(self, error):
        try:
            self.terminate()
        except BaseException:
            # Retain the abort failure as context without replacing an existing
            # explicit cause on the original exception.
            raise error

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        else:
            self._abort_on_error(exc_value)


class _ExclusiveTextWriter(TextIOWrapper):
    def write(self, text):
        try:
            return super().write(text)
        except BaseException as error:
            self.buffer._abort_on_error(error)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        else:
            self.buffer._abort_on_error(exc_value)
