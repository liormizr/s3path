"""
s3path provides a Pythonic API to S3 by wrapping boto3 with pathlib interface
"""
from os import stat_result
from itertools import chain
from functools import lru_cache
from contextlib import suppress
from collections import namedtuple
from platform import python_version
from distutils.version import StrictVersion
from io import DEFAULT_BUFFER_SIZE, UnsupportedOperation
from pathlib import _PosixFlavour, _Accessor, PurePath, Path

try:
    import boto3
    from botocore.exceptions import ClientError
    from botocore.docs.docstring import LazyLoadedDocstring
    import smart_open
except ImportError:
    boto3 = None
    ClientError = Exception
    LazyLoadedDocstring = None
    smart_open = None

__version__ = '0.3.0'
__all__ = (
    'register_configuration_parameter',
    'S3Path',
    'PureS3Path',
    'StatResult',
    'S3DirEntry',
)


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


class _S3ConfigurationMap:
    def __init__(self, default_resource, **default_arguments):
        self.default_resource = default_resource
        self.default_arguments = default_arguments
        self.arguments = None
        self.resources = None

    def _initial_setup(self):
        self.arguments = {PureS3Path('/'): self.default_arguments}
        self.resources = {PureS3Path('/'): self.default_resource}

    def __repr__(self):
        return '{name}(arguments={arguments}, resources={resources})'.format(
            name=type(self).__name__,
            arguments=self.arguments,
            resources=self.resources)

    def set_configuration(self, path, *, resource=None, arguments=None):
        if self.arguments is None and self.resources is None:
            self._initial_setup()
        if arguments is not None:
            self.arguments[path] = arguments
        if resource is not None:
            self.resources[path] = resource

    @lru_cache()
    def get_configuration(self, path):
        if self.arguments is None and self.resources is None:
            self._initial_setup()
        resources = arguments = None
        for path in chain([path], path.parents):
            if resources is None and path in self.resources:
                resources = self.resources[path]
            if arguments is None and path in self.arguments:
                arguments = self.arguments[path]
        return resources, arguments


class _S3Scandir:
    def __init__(self, *, s3_accessor, path):
        self._s3_accessor = s3_accessor
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def __iter__(self):
        bucket_name = self._path.bucket
        resource, _ = self._s3_accessor.configuration_map.get_configuration(self._path)
        if not bucket_name:
            for bucket in resource.buckets.filter(Prefix=str(self._path)):
                yield S3DirEntry(bucket.name, is_dir=True)
            return
        bucket = resource.Bucket(bucket_name)
        sep = self._path._flavour.sep

        kwargs = {
            'Bucket': bucket.name,
            'Prefix': self._s3_accessor.generate_prefix(self._path),
            'Delimiter': sep}

        continuation_token = None
        while True:
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            response = bucket.meta.client.list_objects_v2(**kwargs)
            for folder in response.get('CommonPrefixes', ()):
                full_name = folder['Prefix'][:-1] if folder['Prefix'].endswith(sep) else folder['Prefix']
                name = full_name.split(sep)[-1]
                yield S3DirEntry(name, is_dir=True)
            for file in response.get('Contents', ()):
                name = file['Key'].split(sep)[-1]
                yield S3DirEntry(name=name, is_dir=False, size=file['Size'], last_modified=file['LastModified'])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')


class _S3Accessor(_Accessor):
    """
    An accessor implements a particular (system-specific or not)
    way of accessing paths on the filesystem.

    In this case this will access AWS S3 service
    """

    def __init__(self, **kwargs):
        try:
            self._s3 = boto3.resource('s3', **kwargs)
        except AttributeError:
            self._s3 = None
        self.configuration_map = _S3ConfigurationMap(default_resource=self._s3)

    def stat(self, path):
        resource, _ = self.configuration_map.get_configuration(path)
        object_summery = resource.ObjectSummary(path.bucket, path.key)
        return StatResult(
            size=object_summery.size,
            last_modified=object_summery.last_modified,
        )

    def is_dir(self, path):
        if str(path) == path.root:
            return True
        resource, _ = self.configuration_map.get_configuration(path)
        bucket = resource.Bucket(path.bucket)
        return any(bucket.objects.filter(Prefix=self.generate_prefix(path)))

    def exists(self, path):
        bucket_name = path.bucket
        resource, _ = self.configuration_map.get_configuration(path)
        if not path.key:
            return resource.Bucket(bucket_name) in resource.buckets.all()
        bucket = resource.Bucket(bucket_name)
        key_name = str(path.key)
        for object in bucket.objects.filter(Prefix=key_name):
            if object.key == key_name:
                return True
            if object.key.startswith(key_name + path._flavour.sep):
                return True
        return False

    def scandir(self, path):
        return _S3Scandir(s3_accessor=self, path=path)

    def listdir(self, path):
        with self.scandir(path) as scandir_iter:
            return [entry.name for entry in scandir_iter]

    def open(self, path, *, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        resource, config = self.configuration_map.get_configuration(path)

        def get_resource_kwargs():
            # This is a good example of the complicity of boto3 and botocore
            # resource arguments from the resource object :-/
            # very annoying...

            try:
                access_key = resource.meta.client._request_signer._credentials.access_key
                secret_key = resource.meta.client._request_signer._credentials.secret_key
                token = resource.meta.client._request_signer._credentials.token
            except AttributeError:
                access_key = secret_key = token = None
            return {
                'endpoint_url': resource.meta.client.meta._endpoint_url,
                'config': resource.meta.client._client_config,
                'region_name': resource.meta.client._client_config.region_name,
                'use_ssl': resource.meta.client._endpoint.host.startswith('https'),
                'verify': resource.meta.client._endpoint.http_session._verify,
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key,
                'aws_session_token': token,
            }

        dummy_object = self._s3.Object('bucket', 'key')
        object_kwargs = self._update_kwargs_with_config(
            dummy_object.get, config=config)
        multipart_upload_kwargs = self._update_kwargs_with_config(
            dummy_object.initiate_multipart_upload, config=config)

        file_object = smart_open.open(
            uri=path.as_uri(),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            ignore_ext=True,
            transport_params={
                'session': boto3.DEFAULT_SESSION,
                'resource_kwargs': get_resource_kwargs(),
                'multipart_upload_kwargs': multipart_upload_kwargs,
                'object_kwargs': object_kwargs,
                'defer_seek': True,
            },
        )
        return file_object

    def owner(self, path):
        bucket_name = path.bucket
        key_name = path.key
        resource, _ = self.configuration_map.get_configuration(path)
        object_summery = resource.ObjectSummary(bucket_name, key_name)
        # return object_summery.owner['DisplayName']
        # This is a hack till boto3 resolve this issue:
        # https://github.com/boto/boto3/issues/1950
        responce = object_summery.meta.client.list_objects_v2(
            Bucket=object_summery.bucket_name,
            Prefix=object_summery.key,
            FetchOwner=True)
        return responce['Contents'][0]['Owner']['DisplayName']

    def rename(self, path, target):
        source_bucket_name = path.bucket
        source_key_name = path.key
        target_bucket_name = target.bucket
        target_key_name = target.key

        resource, _ = self.configuration_map.get_configuration(path)

        if not self.is_dir(path):
            target_bucket = resource.Bucket(target_bucket_name)
            object_summery = resource.ObjectSummary(source_bucket_name, source_key_name)
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            self.boto3_method_with_parameters(
                target_bucket.copy,
                args=(old_source, target_key_name))
            self.boto3_method_with_parameters(object_summery.delete)
            return
        bucket = resource.Bucket(source_bucket_name)
        target_bucket = resource.Bucket(target_bucket_name)
        for object_summery in bucket.objects.filter(Prefix=source_key_name):
            old_source = {'Bucket': object_summery.bucket_name, 'Key': object_summery.key}
            new_key = object_summery.key.replace(source_key_name, target_key_name)
            self.boto3_method_with_parameters(
                target_bucket.copy,
                args=(old_source, new_key))
            self.boto3_method_with_parameters(object_summery.delete)

    def replace(self, path, target):
        return self.rename(path, target)

    def rmdir(self, path):
        bucket_name = path.bucket
        key_name = path.key
        resource, config = self.configuration_map.get_configuration(path)
        bucket = resource.Bucket(bucket_name)
        for object_summery in bucket.objects.filter(Prefix=key_name):
            self.boto3_method_with_parameters(object_summery.delete, config=config)

    def mkdir(self, path, mode):
        resource, config = self.configuration_map.get_configuration(path)
        self.boto3_method_with_parameters(
            resource.create_bucket,
            config=config,
            kwargs={'Bucket': path.bucket},
        )

    def boto3_method_with_parameters(self, boto3_method, config=None, args=(), kwargs=None):
        kwargs = self._update_kwargs_with_config(boto3_method, config, kwargs)
        return boto3_method(*args, **kwargs)

    def generate_prefix(self, path):
        sep = path._flavour.sep
        if not path.key:
            return ''
        key_name = path.key
        if not key_name.endswith(sep):
            return key_name + sep
        return key_name

    def unlink(self, path, *args, **kwargs):
        bucket_name = path.bucket
        key_name = path.key
        resource, config = self.configuration_map.get_configuration(path)
        bucket = resource.Bucket(bucket_name)
        try:
            self.boto3_method_with_parameters(
                bucket.meta.client.delete_object,
                config=config,
                kwargs={"Bucket": bucket_name, "Key": key_name}
            )
        except ClientError:
            raise OSError("/{0}/{1}".format(bucket_name, key_name))

    def _update_kwargs_with_config(self, boto3_method, config, kwargs=None):
        kwargs = kwargs or {}
        if config is not None:
            kwargs.update({
                key: value
                for key, value in config.items()
                if key in self._get_action_arguments(boto3_method)
            })
        return kwargs

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

    def chmod(self, mode):
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

    def readlink(self):
        """
        readlink method is unsupported on S3 service
        AWS S3 don't have this file system action concept
        """
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.readlink.__qualname__)
        raise NotImplementedError(message)


_s3_flavour = _S3Flavour()
_s3_accessor = _S3Accessor()


def register_configuration_parameter(path, *, parameters=None, resource=None):
    if not isinstance(path, PureS3Path):
        raise TypeError('path argument have to be a {} type. got {}'.format(PurePath, type(path)))
    if parameters and not isinstance(parameters, dict):
        raise TypeError('parameters argument have to be a dict type. got {}'.format(type(path)))
    if parameters is None and resource is None:
        raise ValueError('user have to specify parameters or resource arguments')
    _s3_accessor.configuration_map.set_configuration(path, resource=resource, arguments=parameters)


class PureS3Path(PurePath):
    """
    PurePath subclass for AWS S3 service.

    S3 is not a file-system but we can look at it like a POSIX system.
    """
    _flavour = _s3_flavour
    __slots__ = ()

    @classmethod
    def from_uri(cls, uri):
        """
        from_uri class method create a class instance from url

        >> from s3path import PureS3Path
        >> PureS3Path.from_url('s3://<bucket>/<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        if not uri.startswith('s3://'):
            raise ValueError('...')
        return cls(uri[4:])

    @property
    def bucket(self):
        """
        The AWS S3 Bucket name, or ''
        """
        self._absolute_path_validation()
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ''

    @property
    def key(self):
        """
        The AWS S3 Key name, or ''
        """
        self._absolute_path_validation()
        key = self._flavour.sep.join(self.parts[2:])
        return key

    @classmethod
    def from_bucket_key(cls, bucket, key):
        """
        from_bucket_key class method create a class instance from bucket, key pair's

        >> from s3path import PureS3Path
        >> PureS3Path.from_bucket_key(bucket='<bucket>', key='<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        bucket = cls(cls._flavour.sep, bucket)
        if len(bucket.parts) != 2:
            raise ValueError('bucket argument contains more then one path element: {}'.format(bucket))
        key = cls(key)
        if key.is_absolute():
            key = key.relative_to('/')
        return bucket / key

    def as_uri(self):
        """
        Return the path as a 's3' URI.
        """
        return super().as_uri()

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')


class S3Path(_PathNotSupportedMixin, Path, PureS3Path):
    """
    Path subclass for AWS S3 service.

    S3Path provide a Python convenient File-System/Path like interface for AWS S3 Service
     using boto3 S3 resource as a driver.

    If boto3 isn't installed in your environment NotImplementedError will be raised.
    """
    __slots__ = ()

    def stat(self):
        """
        Returns information about this path (similarly to boto3's ObjectSummary).
        For compatibility with pathlib, the returned object some similar attributes like os.stat_result.
        The result is looked up at each call to this method
        """
        self._absolute_path_validation()
        if not self.key:
            return None
        return super().stat()

    def exists(self):
        """
        Whether the path points to an existing Bucket, key or key prefix.
        """
        self._absolute_path_validation()
        if not self.bucket:
            return True
        return self._accessor.exists(self)

    def is_dir(self):
        """
        Returns True if the path points to a Bucket or a key prefix, False if it points to a full key path.
        False is also returned if the path doesn’t exist.
        Other errors (such as permission errors) are propagated.
        """
        self._absolute_path_validation()
        if self.bucket and not self.key:
            return True
        return self._accessor.is_dir(self)

    def is_file(self):
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

    def iterdir(self):
        """
        When the path points to a Bucket or a key prefix, yield path objects of the directory contents
        """
        self._absolute_path_validation()
        yield from super().iterdir()

    def glob(self, pattern):
        """
        Glob the given relative pattern in the Bucket / key prefix represented by this path,
        yielding all matching files (of any kind)
        """
        yield from super().glob(pattern)

    def rglob(self, pattern):
        """
        This is like calling S3Path.glob with "**/" added in front of the given relative pattern
        """
        yield from super().rglob(pattern)

    def open(self, mode='r', buffering=DEFAULT_BUFFER_SIZE, encoding=None, errors=None, newline=None):
        """
        Opens the Bucket key pointed to by the path, returns a Key file object that you can read/write with
        """
        self._absolute_path_validation()
        if smart_open.__version__ < '4.0.0' and mode.startswith('b'):
            mode = ''.join(reversed(mode))
        return self._accessor.open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline)

    def owner(self):
        """
        Returns the name of the user owning the Bucket or key.
        Similarly to boto3's ObjectSummary owner attribute
        """
        self._absolute_path_validation()
        if not self.is_file():
            return KeyError('file not found')
        return self._accessor.owner(self)

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
        return super().rename(target)

    def replace(self, target):
        """
        Renames this Bucket / key prefix / key to the given target.
        If target points to an existing Bucket / key prefix / key, it will be unconditionally replaced.
        """
        return self.rename(target)

    def unlink(self, missing_ok=False):
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
        # XXX: Note: If we don't check if the file exists here, S3 will always return
        # success even if we try to delete a key that doesn't exist. So, if we want
        # to raise a `FileNotFoundError`, we need to manually check if the file exists
        # before we make the API call -- since we want to delete the file anyway,
        # we can just ignore this for now and be satisfied that the file will be removed
        if StrictVersion(python_version()) < StrictVersion('3.8'):
            return super().unlink()
        super().unlink(missing_ok=missing_ok)

    def rmdir(self):
        """
        Removes this Bucket / key prefix. The Bucket / key prefix must be empty
        """
        self._absolute_path_validation()
        if self.is_file():
            raise NotADirectoryError()
        if not self.is_dir():
            raise FileNotFoundError()
        return super().rmdir()

    def samefile(self, other_path):
        """
        Returns whether this path points to the same Bucket key as other_path,
        Which can be either a Path object, or a string
        """
        self._absolute_path_validation()
        if not isinstance(other_path, Path):
            other_path = type(self)(other_path)
        return self.bucket == other_path.bucket and self.key == other_path.key and self.is_file()

    def touch(self, mode=0o666, exist_ok=True):
        """
        Creates a key at this given path.
        If the key already exists,
        the function succeeds if exist_ok is true (and its modification time is updated to the current time),
        otherwise FileExistsError is raised
        """
        if self.exists() and not exist_ok:
            raise FileExistsError()
        self.write_text('')

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
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
                raise FileNotFoundError('No bucket in {} {}'.format(type(self), self))
            if self.key and not parents:
                raise FileNotFoundError('Only bucket path can be created, got {}'.format(self))
            if type(self)(self._flavour.sep, self.bucket).exists():
                raise FileExistsError('Bucket {} already exists'.format(self.bucket))
            return super().mkdir(mode, parents=parents, exist_ok=exist_ok)
        except OSError:
            if not exist_ok:
                raise

    def is_mount(self):
        """
        AWS S3 Service doesn't have mounting feature, There for this method will always return False
        """
        return False

    def is_symlink(self):
        """
        AWS S3 Service doesn't have symlink feature, There for this method will always return False
        """
        return False

    def is_socket(self):
        """
        AWS S3 Service doesn't have sockets feature, There for this method will always return False
        """
        return False

    def is_fifo(self):
        """
        AWS S3 Service doesn't have fifo feature, There for this method will always return False
        """
        return False

    def _init(self, template=None):
        super()._init(template)
        if template is None:
            self._accessor = _s3_accessor


class StatResult(namedtuple('BaseStatResult', 'size, last_modified')):
    """
    Base of os.stat_result but with boto3 s3 features
    """

    def __getattr__(self, item):
        if item in vars(stat_result):
            raise UnsupportedOperation('{} do not support {} attribute'.format(type(self).__name__, item))
        return super().__getattribute__(item)

    @property
    def st_size(self):
        return self.size

    @property
    def st_mtime(self):
        return self.last_modified.timestamp()


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
