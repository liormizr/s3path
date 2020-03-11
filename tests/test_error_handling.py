from pathlib import PurePosixPath, PureWindowsPath

import pytest
from gcspath import PureGCSPath


def test_paths_of_a_different_flavour():
    with pytest.raises(TypeError):
        PureGCSPath("/bucket/key") < PurePosixPath("/bucket/key")

    with pytest.raises(TypeError):
        PureWindowsPath("/bucket/key") > PureGCSPath("/bucket/key")
