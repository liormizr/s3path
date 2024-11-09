import sys
import importlib.util
from warnings import warn
from os import stat_result
from threading import Lock
from itertools import chain
from functools import lru_cache
from contextlib import suppress
from collections import namedtuple
from io import UnsupportedOperation


def _lazy_import_resources(name):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.find_spec(name)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


boto3 = _lazy_import_resources('boto3')
smart_open = _lazy_import_resources('smart_open')
# For Development on Cli, or in general application that require fast startup
# This will lazy load boto3 resources
# boto3 increase startup time by X10!


class StatResult(namedtuple('BaseStatResult', 'size, last_modified, version_id', defaults=(None,))):
    """
    Base of os.stat_result but with boto3 s3 features
    """

    def __getattr__(self, item):
        if item in vars(stat_result):
            raise UnsupportedOperation(f'{type(self).__name__} do not support {item} attribute')
        return super().__getattribute__(item)

    @property
    def st_size(self) -> int:
        return self.size

    @property
    def st_mtime(self) -> float:
        return self.last_modified.timestamp()

    @property
    def st_version_id(self) -> str:
        return self.version_id


def stat(path, *, follow_symlinks=True):
    if not follow_symlinks:
        raise NotImplementedError(
            f'Setting follow_symlinks to {follow_symlinks} is unsupported on S3 service.')
    resource, config = configuration_map.get_configuration(path)
    if _is_versioned_path(path):
        object_summary = _boto3_method_with_parameters(
            resource.ObjectVersion(path.bucket, path.key, path.version_id).get,
            config=config,
        )
        return StatResult(
            size=object_summary.get('ContentLength'),
            last_modified=object_summary.get('LastModified'),
            version_id=object_summary.get('VersionId'))
    object_summary = resource.ObjectSummary(path.bucket, path.key)
    return StatResult(
        size=object_summary.size,
        last_modified=object_summary.last_modified,
        version_id=None)


def owner(path):
    bucket_name = path.bucket
    key_name = path.key
    resource, config = configuration_map.get_configuration(path)
    object_summary = resource.ObjectSummary(bucket_name, key_name)
    # return object_summary.owner['DisplayName']
    # This is a hack till boto3 resolve this issue:
    # https://github.com/boto/boto3/issues/1950
    response = _boto3_method_with_parameters(
        object_summary.meta.client.list_objects_v2,
        kwargs={
            'Bucket': object_summary.bucket_name,
            'Prefix': object_summary.key,
            'FetchOwner': True,
        },
        config=config,
    )
    return response['Contents'][0]['Owner']['DisplayName']


def rename(path, target):
    source_bucket_name = path.bucket
    source_key_name = path.key
    target_bucket_name = target.bucket
    target_key_name = target.key

    resource, config = configuration_map.get_configuration(path)
    allowed_copy_args = boto3.s3.transfer.TransferManager.ALLOWED_COPY_ARGS

    if not is_dir(path):
        target_bucket = resource.Bucket(target_bucket_name)
        object_summary = resource.ObjectSummary(source_bucket_name, source_key_name)
        old_source = {'Bucket': object_summary.bucket_name, 'Key': object_summary.key}
        _boto3_method_with_extraargs(
            target_bucket.copy,
            config=config,
            args=(old_source, target_key_name),
            allowed_extra_args=allowed_copy_args)
        _boto3_method_with_parameters(object_summary.delete)
        return

    bucket = resource.Bucket(source_bucket_name)
    target_bucket = resource.Bucket(target_bucket_name)
    for object_summary in bucket.objects.filter(Prefix=source_key_name):
        old_source = {'Bucket': object_summary.bucket_name, 'Key': object_summary.key}
        new_key = object_summary.key.replace(source_key_name, target_key_name)
        _, config = configuration_map.get_configuration(type(path)(target_bucket_name, new_key))
        _boto3_method_with_extraargs(
            target_bucket.copy,
            config=config,
            args=(old_source, new_key),
            allowed_extra_args=allowed_copy_args)
        _boto3_method_with_parameters(object_summary.delete)


replace = rename


def rmdir(path):
    bucket_name = path.bucket
    key_name = path.key
    resource, config = configuration_map.get_configuration(path)
    bucket = resource.Bucket(bucket_name)
    for object_summary in bucket.objects.filter(Prefix=key_name):
        _boto3_method_with_parameters(object_summary.delete, config=config)
    if path.is_bucket:
        _boto3_method_with_parameters(bucket.delete, config=config)


def mkdir(path, mode):
    resource, config = configuration_map.get_configuration(path)
    _boto3_method_with_parameters(
        resource.create_bucket,
        config=config,
        kwargs={'Bucket': path.bucket},
    )


def is_dir(path):
    if str(path) == path.root:
        return True
    resource, config = configuration_map.get_configuration(path)
    bucket = resource.Bucket(path.bucket)
    query = _boto3_method_with_parameters(
        bucket.objects.filter,
        kwargs={'Prefix': _generate_prefix(path)},
        config=config)
    return any(query)


def exists(path):
    bucket_name = path.bucket
    resource, config = configuration_map.get_configuration(path)

    if not path.key:
        # Check whether or not the bucket exists.
        # See https://stackoverflow.com/questions/26871884
        try:
            _boto3_method_with_parameters(
                resource.meta.client.head_bucket,
                kwargs={'Bucket': bucket_name},
                config=config)
            return True
        except Exception as client_error:
            with suppress(AttributeError, KeyError):
                error_code = client_error.response['Error']['Code']
                if error_code == '404':
                    # Not found
                    return False
            raise client_error

    bucket = resource.Bucket(bucket_name)
    key_name = str(path.key)

    def query_method():
        return _boto3_method_with_parameters(
            bucket.object_versions.filter,
            kwargs={'Prefix': key_name},
            config=config)

    if _is_versioned_path(path):
        for object in query_method():
            if object.version_id != path.version_id:
                continue
            if object.key == key_name:
                return True
            if object.key.startswith(key_name + path._flavour.sep):
                return True
        return False

    for object in query_method():
        if object.key == key_name:
            return True
        if object.key.startswith(key_name + path._flavour.sep):
            return True
    return False


def iter_keys(path, *, prefix=None, full_keys=True):
    resource, config = configuration_map.get_configuration(path)
    bucket_name = path.bucket

    def get_keys():
        continuation_token = None
        while True:
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            response = _boto3_method_with_parameters(
                resource.meta.client.list_objects_v2,
                kwargs=kwargs,
                config=config,
            )
            for file in response.get('Contents', ()):
                yield file['Key']
            for folder in response.get('CommonPrefixes', ()):
                yield folder['Prefix']
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    # get buckets
    if not bucket_name and not full_keys:
        query = _boto3_method_with_parameters(
            resource.buckets.filter,
            config=config)
        for bucket in query:
            yield bucket.name
        return
    # get keys in buckets
    if not bucket_name:
        query = _boto3_method_with_parameters(
            resource.buckets.filter,
            config=config)
        for bucket in query:
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


def scandir(path):
    return _S3Scandir(path=path)


def listdir(path):
    with scandir(path) as scandir_iter:
        return [entry.name for entry in scandir_iter]


def open(path, *, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
    resource, config = configuration_map.get_configuration(path)

    dummy_object = resource.Object('bucket', 'key')
    get_object_kwargs = _update_kwargs_with_config(
        dummy_object.meta.client.get_object, config=config)
    create_multipart_upload_kwargs = _update_kwargs_with_config(
        dummy_object.meta.client.create_multipart_upload, config=config)


    transport_params = {'defer_seek': True}
    if _is_versioned_path(path):
        transport_params['version_id'] = path.version_id

    transport_params.update(
        client=resource.meta.client,
        client_kwargs={
            'S3.Client.get_object': get_object_kwargs,
            'S3.Client.create_multipart_upload': create_multipart_upload_kwargs,
        },
    )

    return smart_open.open(
        uri="s3:/" + str(path),
        mode=mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        newline=newline,
        compression='disable',
        transport_params=transport_params)


def get_presigned_url(path, expire_in: int) -> str:
    resource, config = configuration_map.get_configuration(path)
    return _boto3_method_with_parameters(
        resource.meta.client.generate_presigned_url,
        config=config,
        kwargs={
            'ClientMethod': 'get_object',
            'Params': {'Bucket': path.bucket, 'Key': path.key},
            'ExpiresIn': expire_in,
        }
    )


def _generate_prefix(path):
    sep = path._flavour.sep
    if not path.key:
        return ''
    key_name = path.key
    if not key_name.endswith(sep):
        return key_name + sep
    return key_name


def unlink(path, *args, **kwargs):
    bucket_name = path.bucket
    key_name = path.key
    resource, config = configuration_map.get_configuration(path)
    bucket = resource.Bucket(bucket_name)
    try:
        _boto3_method_with_parameters(
            bucket.meta.client.delete_object,
            config=config,
            kwargs={"Bucket": bucket_name, "Key": key_name}
        )
    except ClientError:
        raise OSError(f'/{bucket_name}/{key_name}')


def _is_versioned_path(path):
    return hasattr(path, 'version_id') and bool(path.version_id)


def _update_kwargs_with_config(boto3_method, config, kwargs=None):
    kwargs = kwargs or {}
    if config is not None:
        kwargs.update({
            key: value
            for key, value in config.items()
            if key in _get_action_arguments(boto3_method)
        })
    return kwargs


def _boto3_method_with_parameters(boto3_method, config=None, args=(), kwargs=None):
    kwargs = _update_kwargs_with_config(boto3_method, config, kwargs)
    return boto3_method(*args, **kwargs)


def _boto3_method_with_extraargs(
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


@lru_cache()
def _get_action_arguments(action):
    docs = action.__doc__
    with suppress(AttributeError):
        docs = action.__doc__._generate()
    return set(
        line.replace(':param ', '').strip().strip(':')
        for line in docs.splitlines()
        if line.startswith(':param ')
    )


class _S3Scandir:
    def __init__(self, *, path):
        self._path = path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def __iter__(self):
        bucket_name = self._path.bucket
        resource, config = configuration_map.get_configuration(self._path)
        if not bucket_name:
            query = _boto3_method_with_parameters(
                resource.buckets.all,
                config=config)
            for bucket in query:
                yield _S3DirEntry(bucket.name, is_dir=True)
            return
        bucket = resource.Bucket(bucket_name)
        sep = self._path._flavour.sep

        kwargs = {
            'Bucket': bucket.name,
            'Prefix': _generate_prefix(self._path),
            'Delimiter': sep}

        continuation_token = None
        while True:
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            response = _boto3_method_with_parameters(
                    bucket.meta.client.list_objects_v2,
                    kwargs=kwargs,
                    config=config)

            for folder in response.get('CommonPrefixes', ()):
                full_name = folder['Prefix'][:-1] if folder['Prefix'].endswith(sep) else folder['Prefix']
                name = full_name.split(sep)[-1]
                yield _S3DirEntry(name, is_dir=True)

            for file in response.get('Contents', ()):
                if file['Key'] == response['Prefix']:
                    continue
                name = file['Key'].split(sep)[-1]
                yield _S3DirEntry(name=name, is_dir=False, size=file['Size'], last_modified=file['LastModified'])

            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')


class _S3DirEntry:
    def __init__(self, name, is_dir, size=None, last_modified=None):
        self.name = name
        self._is_dir = is_dir
        self._stat = StatResult(size=size, last_modified=last_modified)

    def __repr__(self):
        return f'{type(self).__name__}(name={self.name}, is_dir={self._is_dir}, stat={self._stat})'

    def inode(self, *args, **kwargs):
        return None

    def is_dir(self, follow_symlinks=False):
        if follow_symlinks:
            raise TypeError('AWS S3 Service does not have symlink feature')
        return self._is_dir

    def is_file(self):
        return not self._is_dir

    def is_symlink(self, *args, **kwargs):
        return False

    def stat(self):
        return self._stat


class _S3ConfigurationMap:
    def __init__(self):
        self.arguments = None
        self.resources = None
        self.general_options = None
        self.setup_lock = Lock()
        self.is_setup = False

    def __repr__(self):
        return f'{type(self).__name__}' \
               f'(arguments={self.arguments}, resources={self.resources}, is_setup={self.is_setup})'

    @property
    def default_resource(self):
        return boto3.resource('s3')

    def set_configuration(self, path, *, resource=None, arguments=None, glob_new_algorithm=None):
        self._delayed_setup()
        path_name = str(path)
        if arguments is not None:
            self.arguments[path_name] = arguments
        if resource is not None:
            self.resources[path_name] = resource
        if glob_new_algorithm is not None:
            warn(f'glob_new_algorithm Configuration is Deprecated, '
                 f'in the new version we use only the new algorithm for Globing', category=DeprecationWarning)
            self.general_options[path_name] = {'glob_new_algorithm': glob_new_algorithm}
        self.get_configuration.cache_clear()

    @lru_cache()
    def get_configuration(self, path):
        self._delayed_setup()
        resources = arguments = None
        for path in chain([path], path.parents):
            path_name = str(path)
            if resources is None and path_name in self.resources:
                resources = self.resources[path_name]
            if arguments is None and path_name in self.arguments:
                arguments = self.arguments[path_name]
        return resources, arguments

    @lru_cache()
    def get_general_options(self, path):
        self._delayed_setup()
        for path in chain([path], path.parents):
            path_name = str(path)
            if path_name in self.general_options:
                return self.general_options[path_name]
        return

    def _delayed_setup(self):
        """ Resolves a circular dependency between us and PureS3Path """
        with self.setup_lock:
            if not self.is_setup:
                self.arguments = {'/': {}}
                self.resources = {'/': self.default_resource}
                self.general_options = {'/': {'glob_new_algorithm': True}}
                self.is_setup = True


configuration_map = _S3ConfigurationMap()
