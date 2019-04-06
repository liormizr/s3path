from pathlib import PurePosixPath, PureWindowsPath

import pytest
from s3path import PureS3Path


def test_paths_of_a_different_flavour():
    with pytest.raises(TypeError):
        PureS3Path('/bucket/key') < PurePosixPath('/bucket/key')

    with pytest.raises(TypeError):
        PureWindowsPath('/bucket/key')> PureS3Path('/bucket/key')
