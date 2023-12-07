from threading import Lock
from itertools import chain
from functools import lru_cache

import boto3


class S3ConfigurationMap:
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
