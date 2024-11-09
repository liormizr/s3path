from __future__ import annotations

import re
import sys
import typing
import fnmatch
import posixpath
from datetime import timedelta
from contextlib import suppress
from urllib.parse import unquote
from pathlib import PurePath, Path
from typing import Union, Literal, Optional
from io import DEFAULT_BUFFER_SIZE, TextIOWrapper

from botocore.exceptions import ClientError

if typing.TYPE_CHECKING:
    import smart_open
    from boto3.resources.factory import ServiceResource
    KeyFileObjectType = Union[TextIOWrapper, smart_open.s3.Reader, smart_open.s3.MultipartWriter]

from . import accessor


def register_configuration_parameter(
        path: PureS3Path,
        *,
        parameters: Optional[dict] = None,
        resource: Optional[ServiceResource] = None,
        glob_new_algorithm: Optional[bool] = None):
    if not isinstance(path, PureS3Path):
        raise TypeError(f'path argument have to be a {PurePath} type. got {type(path)}')
    if parameters and not isinstance(parameters, dict):
        raise TypeError(f'parameters argument have to be a dict type. got {type(path)}')
    if parameters is None and resource is None and glob_new_algorithm is None:
        raise ValueError('user have to specify parameters or resource arguments')
    accessor.configuration_map.set_configuration(
        path,
        resource=resource,
        arguments=parameters,
        glob_new_algorithm=glob_new_algorithm)


class _S3Parser:
    def __getattr__(self, name):
        return getattr(posixpath, name)


class PureS3Path(PurePath):
    """
    PurePath subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.
    """

    parser = _flavour = _S3Parser()  # _flavour is not relevant after Python version 3.13

    __slots__ = ()

    def __init__(self, *args):
        super().__init__(*args)

        new_parts = list(self.parts)
        for part in new_parts[1:]:
            if part == '..':
                index = new_parts.index(part)
                new_parts.pop(index - 1)
                new_parts.remove(part)

        self._raw_paths = new_parts
        if sys.version_info >= (3, 13):
            self._drv, self._root, self._tail_cached = self._parse_path(self._raw_path)
        else:
            self._load_parts()

    @classmethod
    def from_uri(cls, uri: str):
        """
        from_uri class method create a class instance from url

        >> from s3path import PureS3Path
        >> PureS3Path.from_uri('s3://<bucket>/<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        if not uri.startswith('s3://'):
            raise ValueError('Provided uri seems to be no S3 URI!')
        unquoted_uri = unquote(uri)
        return cls(unquoted_uri[4:])

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str):
        """
        from_bucket_key class method create a class instance from bucket, key pair's

        >> from s3path import PureS3Path
        >> PureS3Path.from_bucket_key(bucket='<bucket>', key='<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        bucket = cls(cls.parser.sep, bucket)
        if len(bucket.parts) != 2:
            raise ValueError(f'bucket argument contains more then one path element: {bucket}')
        key = cls(key)
        if key.is_absolute():
            key = key.relative_to('/')
        return bucket / key

    @property
    def bucket(self) -> str:
        """
        The AWS S3 Bucket name, or ''
        """
        self._absolute_path_validation()
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ''

    @property
    def is_bucket(self) -> bool:
        """
        Check if Path is a bucket
        """
        return self.is_absolute() and self == PureS3Path(f"/{self.bucket}")

    @property
    def key(self) -> str:
        """
        The AWS S3 Key name, or ''
        """
        self._absolute_path_validation()
        key = self.parser.sep.join(self.parts[2:])
        return key

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        uri = super().as_uri()
        return uri.replace('file:///', 's3://')

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')


class _PathNotSupportedMixin:
    _NOT_SUPPORTED_MESSAGE = '{method} is unsupported on S3 service'

    @classmethod
    def cwd(cls):
        """
        cwd class method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.cwd.__qualname__)
        raise NotImplementedError(message)

    @classmethod
    def home(cls):
        """
        home class method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.home.__qualname__)
        raise NotImplementedError(message)

    def chmod(self, mode, *, follow_symlinks=True):
        """
        chmod method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.chmod.__qualname__)
        raise NotImplementedError(message)

    def expanduser(self):
        """
        expanduser method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.expanduser.__qualname__)
        raise NotImplementedError(message)

    def lchmod(self, mode):
        """
        lchmod method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lchmod.__qualname__)
        raise NotImplementedError(message)

    def group(self):
        """
        group method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.group.__qualname__)
        raise NotImplementedError(message)

    def is_block_device(self):
        """
        is_block_device method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_block_device.__qualname__)
        raise NotImplementedError(message)

    def is_char_device(self):
        """
        is_char_device method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.is_char_device.__qualname__)
        raise NotImplementedError(message)

    def lstat(self):
        """
        lstat method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lstat.__qualname__)
        raise NotImplementedError(message)

    def resolve(self):
        """
        resolve method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.resolve.__qualname__)
        raise NotImplementedError(message)

    def symlink_to(self, *args, **kwargs):
        """
        symlink_to method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.symlink_to.__qualname__)
        raise NotImplementedError(message)

    def hardlink_to(self, *args, **kwargs):
        """
        hardlink_to method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.hardlink_to.__qualname__)
        raise NotImplementedError(message)

    def readlink(self):
        """
        readlink method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.readlink.__qualname__)
        raise NotImplementedError(message)

    def is_symlink(self) -> Literal[False]:
        """
        AWS S3 Service doesn't have symlink feature, There for this method will always return False
        """
        return False

    def is_socket(self) -> Literal[False]:
        """
        AWS S3 Service doesn't have sockets feature, There for this method will always return False
        """
        return False

    def is_fifo(self) -> Literal[False]:
        """
        AWS S3 Service doesn't have fifo feature, There for this method will always return False
        """
        return False

    def is_mount(self) -> Literal[False]:
        """
        AWS S3 Service doesn't have mounting feature, There for this method will always return False
        """
        return False


class S3Path(_PathNotSupportedMixin, PureS3Path, Path):
    def stat(self, *, follow_symlinks: bool = True) -> accessor.StatResult:
        """
        Returns information about this path (similarly to boto3's ObjectSummary).
        For compatibility with pathlib, the returned object some similar attributes like os.stat_result.
        The result is looked up at each call to this method
        """
        if not follow_symlinks:
            raise NotImplementedError(
                f'Setting follow_symlinks to {follow_symlinks} is unsupported on S3 service.')

        self._absolute_path_validation()
        if not self.key:
            return None
        return accessor.stat(self, follow_symlinks=follow_symlinks)

    def absolute(self) -> S3Path:
        """
        Handle absolute method only if the path is already an absolute one
        since we have no way to compute an absolute path from a relative one in S3.
        """
        if self.is_absolute():
            return self
        # We can't compute the absolute path from a relative one
        raise ValueError("Absolute path can't be determined for relative S3Path objects")

    def owner(self) -> str:
        """
        Returns the name of the user owning the Bucket or key.
        Similarly to boto3's ObjectSummary owner attribute
        """
        self._absolute_path_validation()
        if not self.is_file():
            raise KeyError('file not found')
        return accessor.owner(self)

    def rename(self, target):
        """
        Renames this file or Bucket / key prefix / key to the given target.
        If target exists and is a file, it will be replaced silently if the user has permission.
        If path is a key prefix, it will replace all the keys with the same prefix to the new target prefix.
        Target can be either a string or another S3Path object.
        """
        self._absolute_path_validation()
        if not isinstance(target, type(self)):
            target = type(self)(target)
        target._absolute_path_validation()
        accessor.rename(self, target)
        return type(self)(target)

    def replace(self, target):
        """
        Renames this Bucket / key prefix / key to the given target.
        If target points to an existing Bucket / key prefix / key, it will be unconditionally replaced.
        """
        return self.rename(target)

    def rmdir(self):
        """
        Removes this Bucket / key prefix. The Bucket / key prefix must be empty
        """
        self._absolute_path_validation()
        if self.is_file():
            raise NotADirectoryError()
        if not self.is_dir():
            raise FileNotFoundError()
        accessor.rmdir(self)

    def samefile(self, other_path: Union[str, S3Path]) -> bool:
        """
        Returns whether this path points to the same Bucket key as other_path,
        Which can be either a Path object, or a string
        """
        self._absolute_path_validation()
        if not isinstance(other_path, Path):
            other_path = type(self)(other_path)
        return self.bucket == other_path.bucket and self.key == other_path.key and self.is_file()

    def touch(self, mode: int = 0o666, exist_ok: bool = True):
        """
        Creates a key at this given path.
        If the key already exists,
        the function succeeds if exist_ok is true (and its modification time is updated to the current time),
        otherwise FileExistsError is raised
        """
        if self.exists() and not exist_ok:
            raise FileExistsError()
        self.write_text('')

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False):
        """
        Create a path bucket.
        AWS S3 Service doesn't support folders, therefore the mkdir method will only create the current bucket.
        If the bucket path already exists, FileExistsError is raised.

        If exist_ok is false (the default), FileExistsError is raised if the target Bucket already exists.
        If exist_ok is true, OSError exceptions will be ignored.

        if parents is false (the default), mkdir will create the bucket only if this is a Bucket path.
        if parents is true, mkdir will create the bucket even if the path have a Key path.

        mode argument is ignored.
        """
        try:
            if not self.bucket:
                raise FileNotFoundError(f'No bucket in {type(self)} {self}')
            if self.key and not parents:
                raise FileNotFoundError(f'Only bucket path can be created, got {self}')
            if type(self)(self.parser.sep, self.bucket).exists():
                raise FileExistsError(f'Bucket {self.bucket} already exists')
            accessor.mkdir(self, mode)
        except OSError:
            if not exist_ok:
                raise

    def is_dir(self) -> bool:
        """
        Returns True if the path points to a Bucket or a key prefix, False if it points to a full key path.
        False is also returned if the path doesn’t exist.
        Other errors (such as permission errors) are propagated.
        """
        self._absolute_path_validation()
        if self.bucket and not self.key:
            return True
        return accessor.is_dir(self)

    def is_file(self) -> bool:
        """
        Returns True if the path points to a Bucket key, False if it points to Bucket or a key prefix.
        False is also returned if the path doesn’t exist.
        Other errors (such as permission errors) are propagated.
        """
        self._absolute_path_validation()
        if not self.bucket or not self.key:
            return False
        try:
            return bool(self.stat())
        except ClientError:
            return False

    def exists(self) -> bool:
        """
        Whether the path points to an existing Bucket, key or key prefix.
        """
        self._absolute_path_validation()
        if not self.bucket:
            return True
        return accessor.exists(self)

    def iterdir(self):
        """
        When the path points to a Bucket or a key prefix, yield path objects of the directory contents
        """
        self._absolute_path_validation()
        for name in accessor.listdir(self):
            yield self / name

    def open(
            self,
            mode: Literal['r', 'w', 'rb', 'wb'] = 'r',
            buffering: int = DEFAULT_BUFFER_SIZE,
            encoding: Optional[str] = None,
            errors: Optional[str] = None,
            newline: Optional[str] = None) -> KeyFileObjectType:
        """
        Opens the Bucket key pointed to by the path, returns a Key file object that you can read/write with
        """
        self._absolute_path_validation()
        return accessor.open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline)

    def glob(self, pattern: str, *, case_sensitive=None, recurse_symlinks=False):
        """
        Glob the given relative pattern in the Bucket / key prefix represented by this path,
        yielding all matching files (of any kind)

        The glob method is using a new Algorithm that better fit S3 API
        """
        self._absolute_path_validation()
        if case_sensitive is False or recurse_symlinks is True:
            raise ValueError('Glob is case-sensitive and no symbolic links are allowed')

        sys.audit("pathlib.Path.glob", self, pattern)
        if not pattern:
            raise ValueError(f'Unacceptable pattern: {pattern}')
        drv, root, pattern_parts = self._parse_path(pattern)
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        for part in pattern_parts:
            if part != '**' and '**' in part:
                raise ValueError("Invalid pattern: '**' can only be an entire path component")
        selector = _Selector(self, pattern=pattern)
        yield from selector.select()

    def rglob(self, pattern: str, *, case_sensitive=None, recurse_symlinks=False):
        """
        This is like calling S3Path.glob with "**/" added in front of the given relative pattern

        The rglob method is using a new Algorithm that better fit S3 API
        """
        self._absolute_path_validation()

        sys.audit("pathlib.Path.rglob", self, pattern)
        if not pattern:
            raise ValueError(f'Unacceptable pattern: {pattern}')
        drv, root, pattern_parts = self._parse_path(pattern)
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        for part in pattern_parts:
            if part != '**' and '**' in part:
                raise ValueError("Invalid pattern: '**' can only be an entire path component")
        pattern = f'**{self.parser.sep}{pattern}'
        selector = _Selector(self, pattern=pattern)
        yield from selector.select()

    def get_presigned_url(self, expire_in: Union[timedelta, int] = 3600) -> str:
        """
        Returns a pre-signed url. Anyone with the url can make a GET request to get the file.
        You can set an expiration date with the expire_in argument (integer or timedelta object).

        Note that generating a presigned url may require more information or setup than to use other
        S3Path functions. It's because it needs to know the exact aws region and use s3v4 as signature
        version. Meaning you may have to do this:

        ```python
        import boto3
        from botocore.config import Config
        from s3path import S3Path, register_configuration_parameter

        resource = boto3.resource(
            "s3",
            config=Config(signature_version="s3v4"),
            region_name="the aws region name"
        )
        register_configuration_parameter(S3Path("/"), resource=resource)
        ```

        A simple example:
        ```python
        from s3path import S3Path
        import requests

        file = S3Path("/my-bucket/toto.txt")
        file.write_text("hello world")

        presigned_url = file.get_presigned_url()
        print(requests.get(presigned_url).content)
        b"hello world"
        """
        self._absolute_path_validation()
        if isinstance(expire_in, timedelta):
            expire_in = int(expire_in.total_seconds())
        if expire_in <= 0:
            raise ValueError(
                f"The expire_in argument can't represent a negative or null time delta. "
                f'You provided expire_in = {expire_in} seconds which is below or equal to 0 seconds.')
        return accessor.get_presigned_url(self, expire_in)

    def unlink(self, missing_ok: bool = False):
        """
        Remove this key from its bucket.
        """
        self._absolute_path_validation()
        # S3 doesn't care if you remove full prefixes or buckets with its delete API
        # so unless we manually check, this call will be dropped through without any
        # validation and could result in data loss
        try:
            if self.is_dir():
                raise IsADirectoryError(str(self))
            if not self.is_file():
                raise FileNotFoundError(str(self))
        except (IsADirectoryError, FileNotFoundError):
            if missing_ok:
                return
            raise
        try:
            # XXX: Note: If we don't check if the file exists here, S3 will always return
            # success even if we try to delete a key that doesn't exist. So, if we want
            # to raise a `FileNotFoundError`, we need to manually check if the file exists
            # before we make the API call -- since we want to delete the file anyway,
            # we can just ignore this for now and be satisfied that the file will be removed
            accessor.unlink(self)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def _scandir(self):
        """
        Override _scandir so _Selector will rely on an S3 compliant implementation
        """
        return accessor.scandir(self)


class PureVersionedS3Path(PureS3Path):
    """
    PurePath subclass for AWS S3 service Keys with Versions.

    S3 is not a file-system, but we can look at it like a POSIX system.
    """

    def __new__(cls, *args, version_id: str):
        self = super().__new__(cls, *args)
        self.version_id = version_id
        return self

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self.as_posix()}, version_id={self.version_id})'

    def __truediv__(self, key):
        if not isinstance(key, (PureS3Path, str)):
            return NotImplemented

        key = S3Path(key) if isinstance(key, str) else key
        return key.__rtruediv__(self)

    def __rtruediv__(self, key):
        if not isinstance(key, (PureS3Path, str)):
            return NotImplemented

        new_path = super().__rtruediv__(key)
        new_path.version_id = self.version_id
        return new_path

    @classmethod
    def from_uri(cls, uri: str, *, version_id: str):
        """
        from_uri class method creates a class instance from uri and version id

        >> from s3path import VersionedS3Path
        >> VersionedS3Path.from_uri('s3://<bucket>/<key>', version_id='<version_id>')
        << VersionedS3Path('/<bucket>/<key>', version_id='<version_id>')
        """

        self = PureS3Path.from_uri(uri)
        return cls(self, version_id=version_id)

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str, *, version_id: str):
        """
        from_bucket_key class method creates a class instance from bucket, key and version id

        >> from s3path import VersionedS3Path
        >> VersionedS3Path.from_bucket_key('<bucket>', '<key>', version_id='<version_id>')
        << VersionedS3Path('/<bucket>/<key>', version_id='<version_id>')
        """

        self = PureS3Path.from_bucket_key(bucket=bucket, key=key)
        return cls(self, version_id=version_id)

    def with_segments(self, *pathsegments):
        """Construct a new path object from any number of path-like objects.
        Subclasses may override this method to customize how new path objects
        are created from methods like `iterdir()`.
        """
        return type(self)(*pathsegments, version_id=self.version_id)

    def joinpath(self, *args):
        if not args:
            return self

        new_path = super().joinpath(*args)

        if isinstance(args[-1], PureVersionedS3Path):
            new_path.version_id = args[-1].version_id
        else:
            new_path = S3Path(new_path)

        return new_path


class VersionedS3Path(PureVersionedS3Path, S3Path):
    """
    S3Path subclass for AWS S3 service Keys with Versions.

    >> from s3path import VersionedS3Path
    >> VersionedS3Path('/<bucket>/<key>', version_id='<version_id>')
    << VersionedS3Path('/<bucket>/<key>', version_id='<version_id>')
    """

    def __init__(self, *args, version_id):
        super().__init__(*args)


def _is_wildcard_pattern(pat):
    # Whether this pattern needs actual matching using fnmatch, or can
    # be looked up directly as a file.
    return "*" in pat or "?" in pat or "[" in pat


class _Selector:
    def __init__(self, path, *, pattern):
        self._path = path
        self._prefix, pattern = self._prefix_splitter(pattern)
        self._full_keys = self._calculate_full_or_just_folder(pattern)
        self._target_level = self._calculate_pattern_level(pattern)
        self.match = self._compile_pattern_parts(self._prefix, pattern, path.bucket)

    def select(self):
        for target in self._deep_cached_dir_scan():
            target = f'{self._path.parser.sep}{self._path.bucket}{target}'
            if self.match(target):
                yield type(self._path)(target)

    def _prefix_splitter(self, pattern):
        if not _is_wildcard_pattern(pattern):
            if self._path.key:
                return f'{self._path.key}{self._path.parser.sep}{pattern}', ''
            return pattern, ''

        *_, pattern_parts = self._path._parse_path(pattern)
        prefix = ''
        for index, part in enumerate(pattern_parts):
            if _is_wildcard_pattern(part):
                break
            prefix += f'{part}{self._path.parser.sep}'

        if pattern.startswith(prefix):
            pattern = pattern.replace(prefix, '', 1)

        key_prefix = self._path.key
        if key_prefix:
            prefix = self._path.parser.sep.join((key_prefix, prefix))
        return prefix, pattern

    def _calculate_pattern_level(self, pattern):
        if '**' in pattern:
            return None
        if self._prefix:
            pattern = f'{self._prefix}{self._path.parser.sep}{pattern}'
        *_, pattern_parts = self._path._parse_path(pattern)
        return len(pattern_parts)

    def _calculate_full_or_just_folder(self, pattern):
        if '**' in pattern:
            return True
        *_, pattern_parts = self._path._parse_path(pattern)
        for part in pattern_parts[:-1]:
            if '*' in part:
                return True
        return False

    def _deep_cached_dir_scan(self):
        cache = set()
        prefix_sep_count = self._prefix.count(self._path.parser.sep)
        for key in accessor.iter_keys(self._path, prefix=self._prefix, full_keys=self._full_keys):
            key_sep_count = key.count(self._path.parser.sep) + 1
            key_parts = key.rsplit(self._path.parser.sep, maxsplit=key_sep_count - prefix_sep_count)
            target_path_parts = key_parts[:self._target_level]
            target_path = ''
            for part in target_path_parts:
                if not part:
                    continue
                target_path += f'{self._path.parser.sep}{part}'
                if target_path in cache:
                    continue
                yield target_path
                cache.add(target_path)

    def _compile_pattern_parts(self, prefix, pattern, bucket):
        pattern = self._path.parser.sep.join((
            '',
            bucket,
            prefix,
            pattern,
        ))
        *_, pattern_parts = self._path._parse_path(pattern)

        new_regex_pattern = ''
        for part in pattern_parts:
            if part == self._path.parser.sep:
                continue
            if '**' in part:
                new_regex_pattern += f'{self._path.parser.sep}*(?s:{part.replace("**", ".*")})'
                continue
            if '*' == part:
                new_regex_pattern += f'{self._path.parser.sep}(?s:[^/]+)'
                continue
            new_regex_pattern += f'{self._path.parser.sep}{fnmatch.translate(part)[:-2]}'
        new_regex_pattern += r'/*\Z'
        return re.compile(new_regex_pattern).fullmatch
