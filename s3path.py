"""
This library wrap's the boto3 api to provide a more Pythonice API to S3
"""
from contextlib import suppress
from collections import namedtuple
from posix import DirEntry
from pathlib import _PosixFlavour, _Accessor, _make_selector, _RecursiveWildcardSelector, PurePath, Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception


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
        simple_object = self.s3.ObjectSummary(path.bucket, path.key)
        return StatResult(
            size=simple_object.size,
            last_modified=simple_object.last_modified,
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

    def _generate_prefix(self, path):
        sep = path._flavour.sep
        if not path.key:
            return ''
        if not path.key.endswith(sep):
            return path.key + sep
        return path.key


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


class S3Path(Path, PureS3Path):
    """Path subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.

    # todo: finish the doc's
    # On a POSIX system, instantiating a Path should return this object.
    """
    __slots__ = ()
    _NOT_SUPPORTED_MESSAGE = '{method} is unsupported on S3 service'

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
        if not self.exists():
            return False
        if self.bucket and not self.key:
            return True
        return self._accessor.is_dir(self)

    def is_file(self):
        self._absolute_path_validation()
        if not self.exists():
            return False
        if not self.bucket or not self.key:
            return False
        try:
            return bool(self.stat())
        except ClientError:
            return False

    def iterdir(self):
        self._absolute_path_validation()
        yield from super().iterdir()

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


    def _init(self, template=None):
        super()._init(template)
        if template is None:
            self._accessor = _s3_accessor

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')
