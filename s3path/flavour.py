import re
import sys
import fnmatch
import posixpath
from pathlib import PurePath

try:
    import boto3
    import smart_open

    is_supported = True
except ImportError:
    boto3 = smart_open = None

    is_supported = False


if sys.version_info >= (3, 12):
    def __getattr__(name):
        return getattr(posixpath, name)
else:
    from pathlib import _posix_flavour
    def __getattr__(name):
        return getattr(_posix_flavour, name)


    def parse_parts(parts):
        drv, root, parsed = _posix_flavour.parse_parts(parts)
        for part in parsed[1:]:
            if part == '..':
                index = parsed.index(part)
                parsed.pop(index - 1)
                parsed.remove(part)
        return drv, root, parsed


    def make_uri(path):
        uri = _posix_flavour.make_uri(path)
        return uri.replace('file:///', 's3://')


def compile_pattern_parts(prefix, pattern, bucket):
    pattern = posixpath.sep.join((
        '',
        bucket,
        prefix,
        pattern,
    ))

    *_, pattern_parts = PurePath._parse_path(pattern)
    new_regex_pattern = ''
    for part in pattern_parts:
        if part == posixpath.sep:
            continue
        if '**' in part:
            new_regex_pattern += f'{posixpath.sep}*(?s:{part.replace("**", ".*")})'
            continue
        new_regex_pattern += f'{posixpath.sep}{fnmatch.translate(part)[:-2]}'
    new_regex_pattern += '/*\Z'
    return re.compile(new_regex_pattern).fullmatch
