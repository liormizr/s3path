"""
s3path provides a Pythonic API to S3 by wrapping boto3 with pathlib interface
"""
from contextlib import suppress
from collections import namedtuple
from tempfile import NamedTemporaryFile
from functools import wraps, partial, lru_cache
from pathlib import _PosixFlavour, _Accessor, PurePath, Path
from io import RawIOBase, DEFAULT_BUFFER_SIZE, UnsupportedOperation

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.response import StreamingBody
    from botocore.docs.docstring import LazyLoadedDocstring
except ImportError:
    boto3 = None
    ClientError = Exception
    StreamingBody = object
    LazyLoadedDocstring = type(None)

__version__ = '0.1.04'
__all__ = (
    'register_configuration_parameter',
    'S3Path',
    'PureS3Path',
    'StatResult',
    'S3DirEntry',
    'S3KeyWritableFileObject',
    'S3KeyReadableFileObject',
)

_SUPPORTED_OPEN_MODES = {'r', 'br', 'rb', 'tr', 'rt', 'w', 'wb', 'bw', 'wt', 'tw'}


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


class _S3ConfigurationMap(dict):
    def __missing__(self, path):
        for parent in path.parents:
            if parent in self:
                return self[parent]
        return self.setdefault(Path('/'), {})


class _S3Accessor(_Accessor):
    """
    An accessor implements a particular (system-specific or not)
    way of accessing paths on the filesystem.

    In this case this will access AWS S3 service
    """

    def __init__(self):
        if boto3 is not None:
            self.s3 = boto3.resource('s3')
        self.configuration_map = _S3ConfigurationMap()

    def stat(self, path):
        object_summery = self.s3.ObjectSummary(self._bucket_name(path.bucket), str(path.key))
        return StatResult(
            size=object_summery.size,
            last_modified=object_summery.last_modified,
        )

    def is_dir(self, path):
        bucket = self.s3.Bucket(self._bucket_name(path.bucket))
        return any(bucket.objects.filter(Prefix=self._generate_prefix(path)))

    def exists(self, path):
        bucket_name = self._bucket_name(path.bucket)
        if not bucket_name:
            return any(self.s3.buckets.all())
        if not path.key:
            return self.s3.Bucket(bucket_name) in self.s3.buckets.all()
        bucket = self.s3.Bucket(bucket_name)
        key_name = str(path.key)
        for object in bucket.objects.filter(Prefix=key_name):
            if object.key == key_name:
                return True
            if object.key.startswith(key_name + path._flavour.sep):
                return True
        return False

    def scandir(self, path):
        bucket_name = self._bucket_name(path.bucket)
        if not bucket_name:
            for bucket in self.s3.buckets.all():
                yield S3DirEntry(bucket.name, is_dir=True)
            return
        bucket = self.s3.Bucket(bucket_name)
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
        return [entry.name for entry in self.scandir(path)]

    def open(self, path, *, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        bucket_name = self._bucket_name(path.bucket)
        key_name = str(path.key)
        object_summery = self.s3.ObjectSummary(bucket_name, key_name)
        file_object = S3KeyReadableFileObject if 'r' in mode else S3KeyWritableFileObject
        return file_object(
            object_summery,
            path=path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline)

    def owner(self, path):
        bucket_name = self._bucket_name(path.bucket)
        key_name = str(path.key)
        object_summery = self.s3.ObjectSummary(bucket_name, key_name)
        return object_summery.owner['DisplayName']

    def rename(self, path, target):
        source_bucket_name = self._bucket_name(path.bucket)
        source_key_name = str(path.key)
        target_bucket_name = self._bucket_name(target.bucket)
        target_key_name = str(target.key)

        if not self.is_dir(path):
            target_bucket = self.s3.Bucket(target_bucket_name)
            object_summery = self.s3.ObjectSummary(source_bucket_name, source_key_name)
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            self.boto3_method_with_parameters(
                target_bucket.copy,
                path=target,
                args=(old_source, target_key_name))
            self.boto3_method_with_parameters(object_summery.delete)
            return
        bucket = self.s3.Bucket(source_bucket_name)
        target_bucket = self.s3.Bucket(target_bucket_name)
        for object_summery in bucket.objects.filter(Prefix=source_key_name):
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            new_key = object_summery.key.replace(source_key_name, target_key_name)
            self.boto3_method_with_parameters(
                target_bucket.copy,
                path=S3Path(target_bucket_name, new_key),
                args=(old_source, new_key))
            self.boto3_method_with_parameters(object_summery.delete)

    def replace(self, path, target):
        return self.rename(path, target)

    def rmdir(self, path):
        bucket_name = self._bucket_name(path.bucket)
        key_name = str(path.key)
        bucket = self.s3.Bucket(bucket_name)
        for object_summery in bucket.objects.filter(Prefix=key_name):
            self.boto3_method_with_parameters(object_summery.delete, path=path)

    def mkdir(self, path, mode):
        self.boto3_method_with_parameters(
            self.s3.create_bucket,
            path=path,
            kwargs={'Bucket': self._bucket_name(path.bucket)},
        )

    def _bucket_name(self, path):
        return str(path.bucket)[1:]

    def boto3_method_with_parameters(self, boto3_method, path=Path('/'), args=(), kwargs=None):
        kwargs = kwargs or {}
        kwargs.update({
            key: value
            for key, value in self.configuration_map[path]
            if key in self._get_action_arguments(boto3_method)
        })
        return boto3_method(*args, **kwargs)

    def _generate_prefix(self, path):
        sep = path._flavour.sep
        if not path.key:
            return ''
        key_name = str(path.key)
        if not key_name.endswith(sep):
            return key_name + sep
        return key_name

    @lru_cache()
    def _get_action_arguments(self, action):
        if isinstance(action.__doc__, LazyLoadedDocstring):
            docs = action.__doc__._generate()
        else:
            docs = action.__doc__
        return set(
            line.replace(':param ', '').strip().strip(':')
            for line in docs.splitlines()
            if line.startswith(':param ')
        )


def _string_parser(text, *, mode, encoding):
    if isinstance(text, bytes):
        if 'b' in mode:
            return text
        return text.decode(encoding or 'utf-8')
    if 't' in mode or 'r' == mode:
        return text
    return text.encode(encoding or 'utf-8')


class _PathNotSupportedMixin:
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

    def expanduser(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.expanduser.__qualname__)
        raise NotImplementedError(message)

    def lchmod(self, mode):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lchmod.__qualname__)
        raise NotImplementedError(message)

    def group(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.group.__qualname__)
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

    def resolve(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.resolve.__qualname__)
        raise NotImplementedError(message)

    def symlink_to(self, *args, **kwargs):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.symlink_to.__qualname__)
        raise NotImplementedError(message)

    def unlink(self):
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.unlink.__qualname__)
        raise NotImplementedError(message)


_s3_flavour = _S3Flavour()
_s3_accessor = _S3Accessor()


def register_configuration_parameter(path, *, parameters):
    if not isinstance(path, PureS3Path):
        raise TypeError('path argument have to be a {} type. got {}'.format(PureS3Path, type(path)))
    if not isinstance(parameters, dict):
        raise TypeError('parameters argument have to be a dict type. got {}'.format(type(path)))
    _s3_accessor.configuration_map[path].update(**parameters)


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
        if not uri.startswith('s3://'):
            raise ValueError('...')
        return cls(uri[4:])

    @property
    def bucket(self):
        """
        Returns a Path
        :return:
        """
        self._absolute_path_validation()
        if not self.is_absolute():
            raise ValueError("relative path don't have bucket")
        try:
            _, bucket, *_ = self.parts
        except ValueError:
            return None
        return type(self)(self._flavour.sep, bucket)

    @property
    def key(self):
        self._absolute_path_validation()
        key = self._flavour.sep.join(self.parts[2:])
        if not key:
            return None
        return type(self)(key)

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')


class S3Path(_PathNotSupportedMixin, Path, PureS3Path):
    """Path subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.

    # todo: finish the doc's
    # On a POSIX system, instantiating a Path should return this object.
    """
    __slots__ = ()

    def stat(self):
        self._absolute_path_validation()
        if not self.key:
            return None
        return super().stat()

    def exists(self):
        self._absolute_path_validation()
        if not self.bucket:
            return True
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
        if mode not in _SUPPORTED_OPEN_MODES:
            raise ValueError('supported modes are {} got {}'.format(_SUPPORTED_OPEN_MODES, mode))
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
        return self.bucket == other_path.bucket and self.key == self.key and self.is_file()

    def touch(self, mode=0o666, exist_ok=True):
        if self.exists() and not exist_ok:
            raise FileExistsError()
        self.write_text('')

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        try:
            if self.bucket is None:
                raise FileNotFoundError('No bucket in {} {}'.format(type(self), self))
            if self.key is not None and not parents:
                raise FileNotFoundError('Only bucket path can be created, got {}'.format(self))
            if self.bucket.exists():
                raise FileExistsError('Bucket {} already exists'.format(self.bucket))
            return super().mkdir(mode, parents=parents, exist_ok=exist_ok)
        except OSError:
            if not exist_ok:
                raise

    def is_mount(self):
        return False

    def is_symlink(self):
        return False

    def is_socket(self):
        return False

    def is_fifo(self):
        return False

    def _init(self, template=None):
        super()._init(template)
        if template is None:
            self._accessor = _s3_accessor


class S3KeyWritableFileObject(RawIOBase):
    def __init__(
            self, object_summery, *,
            path,
            mode='w',
            buffering=DEFAULT_BUFFER_SIZE,
            encoding=None,
            errors=None,
            newline=None):
        super().__init__()
        self.object_summery = object_summery
        self.path = path
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
        _s3_accessor.boto3_method_with_parameters(
            self.object_summery.put,
            path=self.path,
            kwargs={'Body': self._cache}
        )

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
            path,
            mode='b',
            buffering=DEFAULT_BUFFER_SIZE,
            encoding=None,
            errors=None,
            newline=None):
        super().__init__()
        self.object_summery = object_summery
        self.path = path
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
                self._streaming_body = _s3_accessor.boto3_method_with_parameters(
                    self.object_summery.get,
                    path=self.path)['Body']
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
