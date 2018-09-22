"""
This library wrap's the boto3 api to provide a more Pythonice API to S3
"""
from pathlib import PurePath, _PosixFlavour

try:
    import boto3
except ImportError:
    boto3 = None


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

_s3_flavour = _S3Flavour()


class PureS3Path(PurePath):
    _flavour = _s3_flavour
    __slots__ = ()

    # @classmethod
    # def _from_parts(cls, args, init=True):
    #     # We need to call _parse_args on the instance, so as to get the
    #     # right flavour.
    #     self = object.__new__(cls)
    #     import ipdb; ipdb.set_trace()
    #     drv, root, parts = self._parse_args(args)
    #     self._drv = drv
    #     self._root = root
    #     self._parts = parts
    #     if init:
    #         self._init()
    #     return self
