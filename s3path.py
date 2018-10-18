"""
This library wrap's the boto3 api to provide a more Pythonice API to S3
"""
from contextlib import suppress
from collections import namedtuple
from functools import wraps, partial
from tempfile import NamedTemporaryFile
from pathlib import _PosixFlavour, _Accessor, PurePath, Path
from io import RawIOBase, DEFAULT_BUFFER_SIZE, UnsupportedOperation

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
except ImportError:
    boto3 = None
    ClientError = Exception
    StreamingBody = object


class _S3Flavour(_PosixFlavour):
    is_supported = bool(boto3)

    def parse_parts(self, parts):
        drv, root, parsed = super().parse_parts(parts)
        for part in parsed[1:]:
            if part == '..':
                index = parsed.index(part)
                parsed.pop(index - 1)
                parsed.remove(part)
        return drv, root, parsed

    def make_uri(self, path):
        uri = super().make_uri(path)
        return uri.replace('file:///', 's3://')

    def casefold(self, s):
        return s

    def casefold_parts(self, parts):
        return parts


class _S3Accessor(_Accessor):
    """
    An accessor implements a particular (system-specific or not)
    way of accessing paths on the filesystem.

    In this case this will access AWS S3 service
    """
    def __init__(self):
        self.s3 = boto3.resource('s3')

    def stat(self, path):
        object_summery = self.s3.ObjectSummary(path.bucket, path.key)
        return StatResult(
            size=object_summery.size,
            last_modified=object_summery.last_modified,
        )

    def is_dir(self, path):
        bucket = self.s3.Bucket(path.bucket)
        return any(bucket.objects.filter(Prefix=self._generate_prefix(path)))

    def exists(self, path):
        if not path.bucket:
            return any(self.s3.buckets.all())
        if not path.key:
            bucket = self.s3.Bucket(path.bucket)
            return any(bucket.objects.all())
        bucket = self.s3.Bucket(path.bucket)
        for object in bucket.objects.filter(Prefix=path.key):
            if object.key == path.key:
                return True
            if object.key.startswith(path.key + path._flavour.sep):
                return True
        return False

    def scandir(self, path):
        if not path.bucket:
            for bucket in self.s3.buckets.all():
                yield S3DirEntry(bucket.name, is_dir=True)
            return
        bucket = self.s3.Bucket(path.bucket)
        sep = path._flavour.sep

        response = bucket.meta.client.list_objects(
            Bucket=bucket.name,
            Prefix=self._generate_prefix(path),
            Delimiter=sep)
        for folder in response.get('CommonPrefixes', ()):
            full_name = folder['Prefix'][:-1] if folder['Prefix'].endswith(sep) else folder['Prefix']
            name = full_name.split(sep)[-1]
            yield S3DirEntry(name, is_dir=True)
        for file in response.get('Contents', ()):
            name = file['Key'].split(sep)[-1]
            yield S3DirEntry(name=name, is_dir=False, size=file['Size'], last_modified=file['LastModified'])

    def listdir(self, path):
        for entry in self.scandir(path):
            yield entry.name

    def open(self, path, *, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        object_summery = self.s3.ObjectSummary(path.bucket, path.key)
        file_object = S3KeyReadableFileObject if 'r' in mode else S3KeyWritableFileObject
        return file_object(
            object_summery,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline)

    def owner(self, path):
        object_summery = self.s3.ObjectSummary(path.bucket, path.key)
        return object_summery.owner['DisplayName']

    def rename(self, path, target):
        if not self.is_dir(path):
            target_bucket = self.s3.Bucket(target.bucket)
            object_summery = self.s3.ObjectSummary(path.bucket, path.key)
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            target_bucket.copy(old_source, target.key)
            object_summery.delete()
            return
        bucket = self.s3.Bucket(path.bucket)
        target_bucket = self.s3.Bucket(target.bucket)
        for object_summery in bucket.objects.filter(Prefix=path.key):
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            new_key = object_summery.key.replace(path.key, target.key)
            target_bucket.copy(old_source, new_key)
            object_summery.delete()

    def replace(self, path, target):
        return self.rename(path, target)

    def rmdir(self, path):
        bucket = self.s3.Bucket(path.bucket)
        for object_summery in bucket.objects.filter(Prefix=path.key):
            object_summery.delete()

    def _generate_prefix(self, path):
        sep = path._flavour.sep
        if not path.key:
            return ''
        if not path.key.endswith(sep):
            return path.key + sep
        return path.key


def _string_parser(text, *, mode, encoding):
    if isinstance(text, bytes):
        if 'b' in mode:
            return text
        return text.decode(encoding or 'utf-8')
    if 't' in mode or 'r' == mode:
        return text
    return text.encode(encoding or 'utf-8')


_s3_flavour = _S3Flavour()
_s3_accessor = _S3Accessor()


class PureS3Path(PurePath):
    """
    PurePath subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.

    # todo: finish the doc's
    # instantiating a PurePath should return this object.
    # However, you can also instantiate it directly on any system.
    """
    _flavour = _s3_flavour
    __slots__ = ()

    @classmethod
    def from_uri(cls, uri):
        if not uri.startswith('s3:/'):
            raise ValueError('...')
        return cls(uri[4:])

    @property
    def bucket(self):
        """
        Returns a Path
        :return:
        """
        if not self.root:
            return ''
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ''

    @property
    def key(self):
        if not self.root:
            return ''
        if not self.bucket:
            return ''
        key_starts_index = self.parts.index(self.bucket) + 1
        return self._flavour.sep.join(self.parts[key_starts_index:])


class PathNotSupportedMixin:
    _NOT_SUPPORTED_MESSAGE = '{method} is unsupported on S3 service'

    @classmethod
    def cwd(cls):
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.cwd.__qualname__)
        raise NotImplementedError(message)

    @classmethod
    def home(cls):
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.home.__qualname__)
        raise NotImplementedError(message)

    def chmod(self, mode):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.chmod.__qualname__)
        raise NotImplementedError(message)

    def lchmod(self, mode):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lchmod.__qualname__)
        raise NotImplementedError(message)

    def group(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.group.__qualname__)
        raise NotImplementedError(message)

    def is_mount(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_mount.__qualname__)
        raise NotImplementedError(message)

    def is_symlink(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_symlink.__qualname__)
        raise NotImplementedError(message)

    def is_socket(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_socket.__qualname__)
        raise NotImplementedError(message)

    def is_fifo(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_fifo.__qualname__)
        raise NotImplementedError(message)

    def is_block_device(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_block_device.__qualname__)
        raise NotImplementedError(message)

    def is_char_device(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_char_device.__qualname__)
        raise NotImplementedError(message)

    def lstat(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lstat.__qualname__)
        raise NotImplementedError(message)

    def mkdir(self, *args, **kwargs):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.mkdir.__qualname__)
        raise NotImplementedError(message)

    def resolve(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.resolve.__qualname__)
        raise NotImplementedError(message)

    def symlink_to(self, *args, **kwargs):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.symlink_to.__qualname__)
        raise NotImplementedError(message)


class S3Path(PathNotSupportedMixin, Path, PureS3Path):
    """Path subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.

    # todo: finish the doc's
    # On a POSIX system, instantiating a Path should return this object.
    """
    __slots__ = ()

    def stat(self):
        self._absolute_path_validation()
        if not self.bucket or not self.key:
            return None
        return super().stat()

    def exists(self):
        self._absolute_path_validation()
        return self._accessor.exists(self)

    def is_dir(self):
        self._absolute_path_validation()
        if self.bucket and not self.key:
            return True
        return self._accessor.is_dir(self)

    def is_file(self):
        self._absolute_path_validation()
        if not self.bucket or not self.key:
            return False
        try:
            return bool(self.stat())
        except ClientError:
            return False

    def iterdir(self):
        self._absolute_path_validation()
        yield from super().iterdir()

    def open(self, mode='r', buffering=DEFAULT_BUFFER_SIZE, encoding=None, errors=None, newline=None):
        self._absolute_path_validation()
        supported_open_modes = ('r', 'br', 'rb', 'tr', 'rt', 'w', 'wb', 'bw', 'wt', 'tw')
        if mode not in supported_open_modes:
            raise ValueError('supported modes are {} got {}'.format(supported_open_modes, mode))
        if buffering == 0 or buffering == 1:
            raise ValueError('supported buffering values are only block sizes, no 0 or 1')
        if 'b' in mode and encoding:
            raise ValueError("binary mode doesn't take an encoding argument")

        if self._closed:
            self._raise_closed()
        return self._accessor.open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline)

    def owner(self):
        self._absolute_path_validation()
        if not self.is_file():
            return KeyError('file not found')
        return self._accessor.owner(self)

    def rename(self, target):
        """
        Need to support file or directory

        """
        self._absolute_path_validation()
        if not isinstance(target, type(self)):
            target = type(self)(target)
        target._absolute_path_validation()
        return super().rename(target)

    def replace(self, target):
        return self.rename(target)

    def rmdir(self):
        self._absolute_path_validation()
        if self.is_file():
            raise NotADirectoryError()
        if not self.is_dir():
            raise FileNotFoundError()
        return super().rmdir()

    def samefile(self, other_path):
        self._absolute_path_validation()
        if not isinstance(other_path, Path):
            other_path = type(self)(other_path)
        return self.bucket == other_path.bucket and self.key == self.key

    def touch(self, mode=0o666, exist_ok=True):
        self.write_text('')

    def _init(self, template=None):
        super()._init(template)
        if template is None:
            self._accessor = _s3_accessor

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')


class S3KeyWritableFileObject(RawIOBase):
    def __init__(
            self, object_summery, *,
            mode='w',
            buffering=DEFAULT_BUFFER_SIZE,
            encoding=None,
            errors=None,
            newline=None):
        super().__init__()
        self.object_summery = object_summery
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._cache = NamedTemporaryFile(
            mode=self.mode + '+',
            buffering=self.buffering,
            encoding=self.encoding,
            newline=self.newline)
        self._string_parser = partial(_string_parser, mode=self.mode, encoding=self.encoding)

    def __getattr__(self, item):
        try:
            return getattr(self._cache, item)
        except AttributeError:
            return super().__getattribute__(item)

    def writable_check(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if not self.writable():
                raise UnsupportedOperation('not writable')
            return method(self, *args, **kwargs)
        return wrapper

    def writable(self, *args, **kwargs):
        return 'w' in self.mode

    @writable_check
    def write(self, text):
        self._cache.write(self._string_parser(text))
        self._cache.seek(0)
        self.object_summery.put(Body=self._cache)

    def writelines(self, lines):
        self.write(self._string_parser('\n').join(self._string_parser(line) for line in lines))

    def readable(self):
        return False

    def read(self, *args, **kwargs):
        raise UnsupportedOperation('not readable')

    def readlines(self, *args, **kwargs):
        raise UnsupportedOperation('not readable')


class S3KeyReadableFileObject(RawIOBase):
    def __init__(
            self, object_summery, *,
            mode='b',
            buffering=DEFAULT_BUFFER_SIZE,
            encoding=None,
            errors=None,
            newline=None):
        super().__init__()
        self.object_summery = object_summery
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._streaming_body = None
        self._string_parser = partial(_string_parser, mode=self.mode, encoding=self.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        return self.readline()

    def __getattr__(self, item):
        try:
            return getattr(self._streaming_body, item)
        except AttributeError:
            return super().__getattribute__(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def readable_check(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if not self.readable():
                raise UnsupportedOperation('not readable')
            return method(self, *args, **kwargs)
        return wrapper

    def readable(self):
        if 'r' not in self.mode:
            return False
        with suppress(ClientError):
            if self._streaming_body is None:
                self._streaming_body = self.object_summery.get()['Body']
            return True
        return False

    @readable_check
    def read(self, *args, **kwargs):
        return self._string_parser(self._streaming_body.read())

    @readable_check
    def readlines(self, *args, **kwargs):
        return [
            line
            for line in iter(self.readline, self._string_parser(''))
        ]

    @readable_check
    def readline(self):
        with suppress(StopIteration, ValueError):
            line = next(self._streaming_body.iter_lines(chunk_size=self.buffering))
            return self._string_parser(line)
        return self._string_parser(b'')

    def write(self, *args, **kwargs):
        raise UnsupportedOperation('not writable')

    def writelines(self, *args, **kwargs):
        raise UnsupportedOperation('not writable')

    def writable(self, *args, **kwargs):
        return False


StatResult = namedtuple('StatResult', 'size, last_modified')


class S3DirEntry:
    def __init__(self, name, is_dir, size=None, last_modified=None):
        self.name = name
        self._is_dir = is_dir
        self._stat = StatResult(size=size, last_modified=last_modified)

    def __repr__(self):
        return '{}(name={}, is_dir={}, stat={})'.format(
            type(self).__name__, self.name, self._is_dir, self._stat)

    def inode(self, *args, **kwargs):
        return None

    def is_dir(self):
        return self._is_dir

    def is_file(self):
        return not self._is_dir

    def is_symlink(self, *args, **kwargs):
        return False

    def stat(self):
        return self._stat
