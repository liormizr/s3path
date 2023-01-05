"""
s3path provides a Pythonic API to S3 by wrapping boto3 with pathlib interface
"""
import re
import sys
import fnmatch
from os import stat_result
from threading import Lock
from itertools import chain
from functools import lru_cache
from contextlib import suppress
from platform import python_version
from collections import namedtuple, deque
from io import DEFAULT_BUFFER_SIZE, UnsupportedOperation
from pathlib import _PosixFlavour, _is_wildcard_pattern, PurePath, Path

import boto3
from botocore.exceptions import ClientError
from botocore.docs.docstring import LazyLoadedDocstring

import smart_open
from packaging.version import Version
from s3transfer.manager import TransferManager

__version__ = '0.4.0'
__all__ = (
    'register_configuration_parameter',
    'S3Path',
    'PureS3Path',
    'StatResult',
    'S3DirEntry',
)

ALLOWED_COPY_ARGS = TransferManager.ALLOWED_COPY_ARGS


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

    def compile_pattern_parts(self, prefix, pattern, bucket):
        pattern = self.sep.join((
            '',
            bucket,
            prefix,
            pattern,
        ))

        *_, pattern_parts = self.parse_parts((pattern,))
        new_regex_pattern = ''
        for part in pattern_parts:
            if part == self.sep:
                continue
            if '**' in part:
                new_regex_pattern += f'{self.sep}*(?s:{part.replace("**", ".*")})'
                continue
            new_regex_pattern += f'{self.sep}{fnmatch.translate(part)[:-2]}'
        new_regex_pattern += '/*\Z'
        return re.compile(new_regex_pattern).fullmatch


class _S3ConfigurationMap:
    def __init__(self, default_resource_kwargs, **default_arguments):
        self.default_resource_kwargs = default_resource_kwargs
        self.default_arguments = default_arguments
        self.arguments = None
        self.resources = None
        self.general_options = None
        self.setup_lock = Lock()
        self.is_setup = False

    @property
    def default_resource(self):
        return boto3.resource('s3', **self.default_resource_kwargs)

    def _delayed_setup(self):
        """ Resolves a circular dependency between us and PureS3Path """
        with self.setup_lock:
            if not self.is_setup:
                self.arguments = {PureS3Path('/'): self.default_arguments}
                self.resources = {PureS3Path('/'): self.default_resource}
                self.general_options = {PureS3Path('/'): {'glob_new_algorithm': True}}
                self.is_setup = True

    def __repr__(self):
        return f'{type(self).__name__}' \
               f'(arguments={self.arguments}, resources={self.resources}, is_setup={self.is_setup})'

    def set_configuration(self, path, *, resource=None, arguments=None, glob_new_algorithm=None):
        self._delayed_setup()
        if arguments is not None:
            self.arguments[path] = arguments
        if resource is not None:
            self.resources[path] = resource
        if glob_new_algorithm is not None:
            self.general_options[path] = {'glob_new_algorithm': glob_new_algorithm}

    @lru_cache()
    def get_configuration(self, path):
        self._delayed_setup()
        resources = arguments = None
        for path in chain([path], path.parents):
            if resources is None and path in self.resources:
                resources = self.resources[path]
            if arguments is None and path in self.arguments:
                arguments = self.arguments[path]
        return resources, arguments

    @lru_cache()
    def get_general_options(self, path):
        self._delayed_setup()
        for path in chain([path], path.parents):
            if path in self.general_options:
                return self.general_options[path]
        return


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
                if file['Key'] == response['Prefix']:
                    continue
                name = file['Key'].split(sep)[-1]
                yield S3DirEntry(name=name, is_dir=False, size=file['Size'], last_modified=file['LastModified'])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')


class _S3Accessor:
    """
    An accessor implements a particular (system-specific or not)
    way of accessing paths on the filesystem.

    In this case this will access AWS S3 service
    """

    def __init__(self, **kwargs):
        self.configuration_map = _S3ConfigurationMap(default_resource_kwargs=kwargs)

    def stat(self, path, *, follow_symlinks=True):
        if not follow_symlinks:
            raise NotImplementedError(
                f'Setting follow_symlinks to {follow_symlinks} is unsupported on S3 service.')
        resource, _ = self.configuration_map.get_configuration(path)
        object_summary = resource.ObjectSummary(path.bucket, path.key)
        return StatResult(
            size=object_summary.size,
            last_modified=object_summary.last_modified,
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
            # Check whether or not the bucket exists.
            # See https://stackoverflow.com/questions/26871884
            try:
                resource.meta.client.head_bucket(Bucket=bucket_name)
                return True
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    # Not found
                    return False
                raise e
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

        smart_open_kwargs = {
            'uri': "s3:/" + str(path),
            'mode': mode,
            'buffering': buffering,
            'encoding': encoding,
            'errors': errors,
            'newline': newline,
        }
        transport_params = {'defer_seek': True}
        dummy_object = resource.Object('bucket', 'key')
        if smart_open.__version__ >= '5.1.0':
            self._smart_open_new_version_kwargs(
                dummy_object,
                resource,
                config,
                transport_params,
                smart_open_kwargs)
        else:
            self._smart_open_old_version_kwargs(
                dummy_object,
                resource,
                config,
                transport_params,
                smart_open_kwargs)

        file_object = smart_open.open(**smart_open_kwargs)
        return file_object

    def owner(self, path):
        bucket_name = path.bucket
        key_name = path.key
        resource, _ = self.configuration_map.get_configuration(path)
        object_summary = resource.ObjectSummary(bucket_name, key_name)
        # return object_summary.owner['DisplayName']
        # This is a hack till boto3 resolve this issue:
        # https://github.com/boto/boto3/issues/1950
        responce = object_summary.meta.client.list_objects_v2(
            Bucket=object_summary.bucket_name,
            Prefix=object_summary.key,
            FetchOwner=True)
        return responce['Contents'][0]['Owner']['DisplayName']

    def rename(self, path, target):
        source_bucket_name = path.bucket
        source_key_name = path.key
        target_bucket_name = target.bucket
        target_key_name = target.key

        resource, config = self.configuration_map.get_configuration(path)

        if not self.is_dir(path):
            target_bucket = resource.Bucket(target_bucket_name)
            object_summary = resource.ObjectSummary(source_bucket_name, source_key_name)
            old_source = {'Bucket': object_summary.bucket_name, 'Key': object_summary.key}
            self._boto3_method_with_extraargs(
                target_bucket.copy,
                config=config,
                args=(old_source, target_key_name),
                allowed_extra_args=ALLOWED_COPY_ARGS,
            )
            self._boto3_method_with_parameters(object_summary.delete)
            return
        bucket = resource.Bucket(source_bucket_name)
        target_bucket = resource.Bucket(target_bucket_name)
        for object_summary in bucket.objects.filter(Prefix=source_key_name):
            old_source = {'Bucket': object_summary.bucket_name, 'Key': object_summary.key}
            new_key = object_summary.key.replace(source_key_name, target_key_name)
            _, config = self.configuration_map.get_configuration(S3Path(target_bucket_name, new_key))
            self._boto3_method_with_extraargs(
                target_bucket.copy,
                config=config,
                args=(old_source, new_key),
                allowed_extra_args=ALLOWED_COPY_ARGS,
            )
            self._boto3_method_with_parameters(object_summary.delete)

    def replace(self, path, target):
        return self.rename(path, target)

    def rmdir(self, path):
        bucket_name = path.bucket
        key_name = path.key
        resource, config = self.configuration_map.get_configuration(path)
        bucket = resource.Bucket(bucket_name)
        for object_summary in bucket.objects.filter(Prefix=key_name):
            self._boto3_method_with_parameters(object_summary.delete, config=config)

    def mkdir(self, path, mode):
        resource, config = self.configuration_map.get_configuration(path)
        self._boto3_method_with_parameters(
            resource.create_bucket,
            config=config,
            kwargs={'Bucket': path.bucket},
        )

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
            self._boto3_method_with_parameters(
                bucket.meta.client.delete_object,
                config=config,
                kwargs={"Bucket": bucket_name, "Key": key_name}
            )
        except ClientError:
            raise OSError(f'/{bucket_name}/{key_name}')

    def iter_keys(self, path, *, prefix=None, full_keys=True):
        resource, _ = self.configuration_map.get_configuration(path)
        bucket_name = path.bucket

        def get_keys():
            continuation_token = None
            while True:
                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token
                response = resource.meta.client.list_objects_v2(**kwargs)
                for file in response.get('Contents', ()):
                    yield file['Key']
                for folder in response.get('CommonPrefixes', ()):
                    yield folder['Prefix']
                if not response.get('IsTruncated'):
                    break
                continuation_token = response.get('NextContinuationToken')

        # get buckets
        if not bucket_name and not full_keys:
            for bucket in resource.buckets.filter():
                yield bucket.name
            return
        # get keys in buckets
        if not bucket_name:
            for bucket in resource.buckets.filter():
                kwargs = {'Bucket': bucket.name}
                yield from get_keys()
            return
        # get keys or part of keys in buckets
        kwargs = {'Bucket': bucket_name}
        if prefix:
            kwargs['Prefix'] = prefix
        if not full_keys:
            kwargs['Delimiter'] = path._flavour.sep
        yield from get_keys()

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

    def _boto3_method_with_parameters(self, boto3_method, config=None, args=(), kwargs=None):
        kwargs = self._update_kwargs_with_config(boto3_method, config, kwargs)
        return boto3_method(*args, **kwargs)

    def _boto3_method_with_extraargs(
        self,
        boto3_method,
        config=None,
        args=(),
        kwargs=None,
        extra_args=None,
        allowed_extra_args=()):
        kwargs = kwargs or {}
        extra_args = extra_args or {}
        if config is not None:
            extra_args.update({
                key: value
                for key, value in config.items()
                if key in allowed_extra_args
            })
        kwargs["ExtraArgs"] = extra_args
        return boto3_method(*args, **kwargs)

    def _smart_open_new_version_kwargs(
            self,
            dummy_object,
            resource,
            config,
            transport_params,
            smart_open_kwargs):
        """
        New Smart-Open api
        Doc: https://github.com/RaRe-Technologies/smart_open/blob/develop/MIGRATING_FROM_OLDER_VERSIONS.rst
        """
        get_object_kwargs = self._update_kwargs_with_config(
            dummy_object.meta.client.get_object, config=config)
        create_multipart_upload_kwargs = self._update_kwargs_with_config(
            dummy_object.meta.client.create_multipart_upload, config=config)
        transport_params.update(
            client=resource.meta.client,
            client_kwargs={
                'S3.Client.create_multipart_upload': create_multipart_upload_kwargs,
                'S3.Client.get_object': get_object_kwargs
            },
        )
        smart_open_kwargs.update(
            compression='disable',
            transport_params=transport_params,
        )

    def _smart_open_old_version_kwargs(
            self,
            dummy_object,
            resource,
            config,
            transport_params,
            smart_open_kwargs):
        """
        Old Smart-Open api
        <5.0.0
        """
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

        initiate_multipart_upload_kwargs = self._update_kwargs_with_config(
            dummy_object.initiate_multipart_upload, config=config)
        object_kwargs = self._update_kwargs_with_config(dummy_object.get, config=config)
        transport_params.update(
            multipart_upload_kwargs=initiate_multipart_upload_kwargs,
            object_kwargs=object_kwargs,
            resource_kwargs=get_resource_kwargs(),
            session=boto3.DEFAULT_SESSION,
        )
        smart_open_kwargs.update(
            ignore_ext=True,
            transport_params=transport_params,
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


class _Selector:
    def __init__(self, path, *, pattern):
        self._path = path
        self._prefix, pattern = self._prefix_splitter(pattern)
        self._full_keys = self._calculate_full_or_just_folder(pattern)
        self._target_level = self._calculate_pattern_level(pattern)
        self.match = self._path._flavour.compile_pattern_parts(self._prefix, pattern, path.bucket)

    def select(self):
        for target in self._deep_cached_dir_scan():
            target = self._path._flavour.sep.join(('', self._path.bucket, target))
            if self.match(target):
                yield type(self._path)(target)

    def _prefix_splitter(self, pattern):
        *_, pattern_parts = self._path._flavour.parse_parts((pattern,))
        prefix = ''
        key_prefix = self._path.key
        for part in pattern_parts:
            if _is_wildcard_pattern(part):
                break
            if prefix:
                prefix += f'{self._path._flavour.sep}{part}'
            else:
                prefix = part
        prefix_folder = f'{prefix}{self._path._flavour.sep}'
        if prefix_folder == pattern:
            prefix = prefix_folder
        if key_prefix:
            prefix = f'{key_prefix}{self._path._flavour.sep}{prefix}'
        if pattern.startswith(prefix):
            pattern = pattern.replace(prefix, '', 1)
        return prefix, pattern

    def _calculate_pattern_level(self, pattern):
        if '**' in pattern:
            return None
        if self._prefix:
            pattern = f'{self._prefix}{self._path._flavour.sep}{pattern}'
        *_, pattern_parts = self._path._flavour.parse_parts((pattern,))
        index = 0
        for index, part in enumerate(reversed(pattern_parts), 1):
            if not _is_wildcard_pattern(part):
                break
        return index

    def _calculate_full_or_just_folder(self, pattern):
        if '**' in pattern:
            return True
        *_, pattern_parts = self._path._flavour.parse_parts((pattern,))
        for part in pattern_parts[:-1]:
            if '*' in part:
                return True
        return False

    def _deep_cached_dir_scan(self):
        cache = _DeepDirCache()
        prefix_sep_count = self._prefix.count(self._path._flavour.sep)
        for key in self._path._accessor.iter_keys(self._path, prefix=self._prefix, full_keys=self._full_keys):
            key_sep_count = key.count(self._path._flavour.sep) + 1
            key_parts = key.rsplit(self._path._flavour.sep, maxsplit=key_sep_count - prefix_sep_count)
            key_parts_count = sum(1 for _ in key.split(self._path._flavour.sep) if _)
            for index in range(prefix_sep_count, key_parts_count + 1):
                if self._target_level and self._target_level < index:
                    break
                target_path_parts = key_parts[:index]
                target_path = (self._path._flavour.sep).join(target_path_parts)
                if cache.in_cache(target_path):
                    continue
                if self._target_level is None or self._target_level == index:
                    yield target_path
                cache.add(target_path_parts, target_path)


class _DeepDirCache:
    def __init__(self):
        self._queue = deque()
        self._tree = {}

    def __repr__(self):
        return f'{type(self).__name__}{self._tree, self._queue}'

    def in_cache(self, directory):
        return directory in self._queue

    def add(self, directory_parts, directory):
        tree = self._tree
        for part in directory_parts:
            if part in tree:
                tree = tree[part]
                continue
            if tree:
                deep_count = self._deep_count(tree)
                tree.clear()
                for _ in range(deep_count):
                    self._queue.pop()
            tree[part] = {}
        self._queue.append(directory)

    def _deep_count(self, tree):
        count = 0
        while True:
            try:
                tree = next(iter(tree.values()))
            except StopIteration:
                return count
            count += 1


_s3_flavour = _S3Flavour()
_s3_accessor = _S3Accessor()


def register_configuration_parameter(path, *, parameters=None, resource=None, glob_new_algorithm=None):
    if not isinstance(path, PureS3Path):
        raise TypeError(f'path argument have to be a {PurePath} type. got {type(path)}')
    if parameters and not isinstance(parameters, dict):
        raise TypeError(f'parameters argument have to be a dict type. got {type(path)}')
    if parameters is None and resource is None and glob_new_algorithm is None:
        raise ValueError('user have to specify parameters or resource arguments')
    _s3_accessor.configuration_map.set_configuration(
        path,
        resource=resource,
        arguments=parameters,
        glob_new_algorithm=glob_new_algorithm)


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
            raise ValueError('Provided uri seems to be no S3 URI!')
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
            raise ValueError(f'bucket argument contains more then one path element: {bucket}')
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
    _accessor = _s3_accessor
    __slots__ = ()

    def _init(self, template=None):
        super()._init(template)
        if template is None:
            self._accessor = _s3_accessor

    def stat(self, *, follow_symlinks=True):
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
        return self._accessor.stat(self, follow_symlinks=follow_symlinks)

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
        for name in self._accessor.listdir(self):
            yield self._make_child_relpath(name)

    def glob(self, pattern):
        """
        Glob the given relative pattern in the Bucket / key prefix represented by this path,
        yielding all matching files (of any kind)
        """
        self._absolute_path_validation()
        general_options = self._accessor.configuration_map.get_general_options(self)
        glob_new_algorithm = general_options['glob_new_algorithm']
        if not glob_new_algorithm:
            yield from super().glob(pattern)
            return
        yield from self._glob(pattern)

    def _glob(self, pattern):
        """ Glob with new Algorithm that better fit S3 API """
        if Version(python_version()) >= Version('3.8'):
            sys.audit("pathlib.Path.glob", self, pattern)
        if not pattern:
            raise ValueError(f'Unacceptable pattern: {pattern}')
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        for part in pattern_parts:
            if part != '**' and '**' in part:
                raise ValueError("Invalid pattern: '**' can only be an entire path component")
        selector = _Selector(self, pattern=pattern)
        yield from selector.select()

    def _scandir(self):
        """
        Override _scandir so _Selector will rely on an S3 compliant implementation
        """
        return self._accessor.scandir(self)

    def rglob(self, pattern):
        """
        This is like calling S3Path.glob with "**/" added in front of the given relative pattern
        """
        self._absolute_path_validation()
        general_options = self._accessor.configuration_map.get_general_options(self)
        glob_new_algorithm = general_options['glob_new_algorithm']
        if not glob_new_algorithm:
            yield from super().rglob(pattern)
            return
        yield from self._rglob(pattern)

    def _rglob(self, pattern):
        """ RGlob with new Algorithm that better fit S3 API """
        if Version(python_version()) >= Version('3.8'):
            sys.audit("pathlib.Path.rglob", self, pattern)
        if not pattern:
            raise ValueError(f'Unacceptable pattern: {pattern}')
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root:
            raise NotImplementedError("Non-relative patterns are unsupported")
        for part in pattern_parts:
            if part != '**' and '**' in part:
                raise ValueError("Invalid pattern: '**' can only be an entire path component")
        pattern = f'**{self._flavour.sep}{pattern}'
        selector = _Selector(self, pattern=pattern)
        yield from selector.select()

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
        self._accessor.rename(self, target)
        return self.__class__(target)

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
        try:
            # XXX: Note: If we don't check if the file exists here, S3 will always return
            # success even if we try to delete a key that doesn't exist. So, if we want
            # to raise a `FileNotFoundError`, we need to manually check if the file exists
            # before we make the API call -- since we want to delete the file anyway,
            # we can just ignore this for now and be satisfied that the file will be removed
            self._accessor.unlink(self)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rmdir(self):
        """
        Removes this Bucket / key prefix. The Bucket / key prefix must be empty
        """
        self._absolute_path_validation()
        if self.is_file():
            raise NotADirectoryError()
        if not self.is_dir():
            raise FileNotFoundError()
        self._accessor.rmdir(self)

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
                raise FileNotFoundError(f'No bucket in {type(self)} {self}')
            if self.key and not parents:
                raise FileNotFoundError(f'Only bucket path can be created, got {self}')
            if type(self)(self._flavour.sep, self.bucket).exists():
                raise FileExistsError(f'Bucket {self.bucket} already exists')
            self._accessor.mkdir(self, mode)
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

    def absolute(self):
        """
        Handle absolute method only if the path is already an absolute one
        since we have no way to compute an absolute path from a relative one in S3.
        """
        if self.is_absolute():
            return self
        # We can't compute the absolute path from a relative one
        raise ValueError("Absolute path can't be determined for relative S3Path objects")


class StatResult(namedtuple('BaseStatResult', 'size, last_modified')):
    """
    Base of os.stat_result but with boto3 s3 features
    """

    def __getattr__(self, item):
        if item in vars(stat_result):
            raise UnsupportedOperation(f'{type(self).__name__} do not support {item} attribute')
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
        return f'{type(self).__name__}(name={self.name}, is_dir={self._is_dir}, stat={self._stat})'

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
