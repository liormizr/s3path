"""
s3path provides a Pythonic API to S3 by wrapping boto3 with pathlib interface
"""
import sys

from . import accessor

__version__ = '0.5.6'
__all__ = (
    'register_configuration_parameter',
    'StatResult',
    'PureS3Path',
    'S3Path',
    'VersionedS3Path',
    'PureVersionedS3Path',
)

if sys.version_info >= (3, 12):
    from .accessor import StatResult, configuration_map
    from .current_version import (
        S3Path,
        PureS3Path,
        VersionedS3Path,
        PureVersionedS3Path,
        register_configuration_parameter,
    )
else:
    from .old_versions import (
        StatResult,
        S3Path,
        PureS3Path,
        VersionedS3Path,
        PureVersionedS3Path,
        configuration_map,
        register_configuration_parameter,
    )
