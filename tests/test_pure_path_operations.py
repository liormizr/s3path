import os
import sys
import pytest
from pathlib import Path
from gcspath import PureGCSPath


def test_repr():
    assert repr(PureGCSPath("setup.py")) == "PureGCSPath('setup.py')"
    assert str(PureGCSPath("setup.py")) == "setup.py"
    assert bytes(PureGCSPath("setup.py")) == b"setup.py"
    assert PureGCSPath("/usr/bin").as_posix() == "/usr/bin"


@pytest.mark.skipif(sys.version_info < (3, 6), reason="requires python3.6 or higher")
def test_fspath():
    assert os.fspath(PureGCSPath("/usr/bin")) == "/usr/bin"


def test_join_strs():
    assert PureGCSPath("foo", "some/path", "bar") == PureGCSPath("foo/some/path/bar")


def test_join_paths():
    assert PureGCSPath(Path("foo"), Path("bar")) == PureGCSPath("foo/bar")


def test_empty():
    assert PureGCSPath() == PureGCSPath(".")


def test_absolute_paths():
    assert PureGCSPath("/etc", "/usr", "lib64") == PureGCSPath("/usr/lib64")


def test_slashes_single_double_dots():
    assert PureGCSPath("foo//bar") == PureGCSPath("foo/bar")
    assert PureGCSPath("foo/./bar") == PureGCSPath("foo/bar")
    assert PureGCSPath("foo/../bar") == PureGCSPath("bar")
    assert PureGCSPath("../bar") == PureGCSPath("../bar")
    assert PureGCSPath("foo", "../bar") == PureGCSPath("bar")


def test_operators():
    assert PureGCSPath("/etc") / "init.d" / "apache2" == PureGCSPath(
        "/etc/init.d/apache2"
    )
    assert "/usr" / PureGCSPath("bin") == PureGCSPath("/usr/bin")


def test_parts():
    assert PureGCSPath("foo//bar").parts == ("foo", "bar")
    assert PureGCSPath("foo/./bar").parts == ("foo", "bar")
    assert PureGCSPath("foo/../bar").parts == ("bar",)
    assert PureGCSPath("../bar").parts == ("..", "bar")
    assert PureGCSPath("foo", "../bar").parts == ("bar",)
    assert PureGCSPath("/foo/bar").parts == ("/", "foo", "bar")


def test_drive():
    assert PureGCSPath("foo//bar").drive == ""
    assert PureGCSPath("foo/./bar").drive == ""
    assert PureGCSPath("foo/../bar").drive == ""
    assert PureGCSPath("../bar").drive == ""
    assert PureGCSPath("foo", "../bar").drive == ""
    assert PureGCSPath("/foo/bar").drive == ""


def test_root():
    assert PureGCSPath("foo//bar").root == ""
    assert PureGCSPath("foo/./bar").root == ""
    assert PureGCSPath("foo/../bar").root == ""
    assert PureGCSPath("../bar").root == ""
    assert PureGCSPath("foo", "../bar").root == ""
    assert PureGCSPath("/foo/bar").root == "/"


def test_anchor():
    assert PureGCSPath("foo//bar").anchor == ""
    assert PureGCSPath("foo/./bar").anchor == ""
    assert PureGCSPath("foo/../bar").anchor == ""
    assert PureGCSPath("../bar").anchor == ""
    assert PureGCSPath("foo", "../bar").anchor == ""
    assert PureGCSPath("/foo/bar").anchor == "/"


def test_parents():
    assert tuple(PureGCSPath("foo//bar").parents) == (
        PureGCSPath("foo"),
        PureGCSPath("."),
    )
    assert tuple(PureGCSPath("foo/./bar").parents) == (
        PureGCSPath("foo"),
        PureGCSPath("."),
    )
    assert tuple(PureGCSPath("foo/../bar").parents) == (PureGCSPath("."),)
    assert tuple(PureGCSPath("../bar").parents) == (PureGCSPath(".."), PureGCSPath("."))
    assert tuple(PureGCSPath("foo", "../bar").parents) == (PureGCSPath("."),)
    assert tuple(PureGCSPath("/foo/bar").parents) == (
        PureGCSPath("/foo"),
        PureGCSPath("/"),
    )


def test_parent():
    assert PureGCSPath("foo//bar").parent == PureGCSPath("foo")
    assert PureGCSPath("foo/./bar").parent == PureGCSPath("foo")
    assert PureGCSPath("foo/../bar").parent == PureGCSPath(".")
    assert PureGCSPath("../bar").parent == PureGCSPath("..")
    assert PureGCSPath("foo", "../bar").parent == PureGCSPath(".")
    assert PureGCSPath("/foo/bar").parent == PureGCSPath("/foo")
    assert PureGCSPath(".").parent == PureGCSPath(".")
    assert PureGCSPath("/").parent == PureGCSPath("/")


def test_name():
    assert PureGCSPath("my/library/setup.py").name == "setup.py"


def test_suffix():
    assert PureGCSPath("my/library/setup.py").suffix == ".py"
    assert PureGCSPath("my/library.tar.gz").suffix == ".gz"
    assert PureGCSPath("my/library").suffix == ""


def test_suffixes():
    assert PureGCSPath("my/library.tar.gar").suffixes == [".tar", ".gar"]
    assert PureGCSPath("my/library.tar.gz").suffixes == [".tar", ".gz"]
    assert PureGCSPath("my/library").suffixes == []


def test_stem():
    assert PureGCSPath("my/library.tar.gar").stem == "library.tar"
    assert PureGCSPath("my/library.tar").stem == "library"
    assert PureGCSPath("my/library").stem == "library"


def test_uri():
    assert PureGCSPath("/etc/passwd").as_uri() == "gs://etc/passwd"
    assert PureGCSPath("/etc/init.d/apache2").as_uri() == "gs://etc/init.d/apache2"
    assert PureGCSPath("/bucket/key").as_uri() == "gs://bucket/key"


def test_absolute():
    assert PureGCSPath("/a/b").is_absolute()
    assert not PureGCSPath("a/b").is_absolute()


def test_reserved():
    assert not PureGCSPath("/a/b").is_reserved()
    assert not PureGCSPath("a/b").is_reserved()


def test_joinpath():
    assert PureGCSPath("/etc").joinpath("passwd") == PureGCSPath("/etc/passwd")
    assert PureGCSPath("/etc").joinpath(PureGCSPath("passwd")) == PureGCSPath(
        "/etc/passwd"
    )
    assert PureGCSPath("/etc").joinpath("init.d", "apache2") == PureGCSPath(
        "/etc/init.d/apache2"
    )


def test_match():
    assert PureGCSPath("a/b.py").match("*.py")
    assert PureGCSPath("/a/b/c.py").match("b/*.py")
    assert not PureGCSPath("/a/b/c.py").match("a/*.py")
    assert PureGCSPath("/a.py").match("/*.py")
    assert not PureGCSPath("a/b.py").match("/*.py")
    assert not PureGCSPath("a/b.py").match("*.Py")


def test_relative_to():
    s3_path = PureGCSPath("/etc/passwd")
    assert s3_path.relative_to("/") == PureGCSPath("etc/passwd")
    assert s3_path.relative_to("/etc") == PureGCSPath("passwd")
    with pytest.raises(ValueError):
        s3_path.relative_to("/usr")


def test_with_name():
    s3_path = PureGCSPath("/Downloads/pathlib.tar.gz")
    assert s3_path.with_name("setup.py") == PureGCSPath("/Downloads/setup.py")
    s3_path = PureGCSPath("/")
    with pytest.raises(ValueError):
        s3_path.with_name("setup.py")


def test_with_suffix():
    s3_path = PureGCSPath("/Downloads/pathlib.tar.gz")
    assert s3_path.with_suffix(".bz2") == PureGCSPath("/Downloads/pathlib.tar.bz2")
    s3_path = PureGCSPath("README")
    assert s3_path.with_suffix(".txt") == PureGCSPath("README.txt")
    s3_path = PureGCSPath("README.txt")
    assert s3_path.with_suffix("") == PureGCSPath("README")
