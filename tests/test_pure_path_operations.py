import os
import sys
import pytest
from pathlib import Path, PurePosixPath, PureWindowsPath
from s3path import PureS3Path


def test_paths_of_a_different_flavour():
    with pytest.raises(TypeError):
        PureS3Path('/bucket/key') < PurePosixPath('/bucket/key')

    with pytest.raises(TypeError):
        PureWindowsPath('/bucket/key') > PureS3Path('/bucket/key')


def test_repr():
    assert repr(PureS3Path('setup.py')) == "PureS3Path('setup.py')"
    assert str(PureS3Path('setup.py')) == 'setup.py'
    assert bytes(PureS3Path('setup.py')) == b'setup.py'
    assert PureS3Path('/usr/bin').as_posix() == '/usr/bin'


def test_fspath():
    assert os.fspath(PureS3Path('/usr/bin')) == '/usr/bin'


def test_from_uri_issue_150():
    uri = 's3://bucket/test/2023-09-10T00%3A00%3A00.000Z.txt'
    string = '/bucket/test/2023-09-10T00:00:00.000Z.txt'
    path = PureS3Path.from_uri(uri)
    assert path.as_uri() == uri
    assert str(path) == string


def test_join_strs():
    assert PureS3Path('foo', 'some/path', 'bar') == PureS3Path('foo/some/path/bar')


def test_join_paths():
    assert PureS3Path(Path('foo'), Path('bar')) == PureS3Path('foo/bar')


def test_empty():
    assert PureS3Path() == PureS3Path('.')


def test_absolute_paths():
    assert PureS3Path('/etc', '/usr', 'lib64') == PureS3Path('/usr/lib64')


def test_slashes_single_double_dots():
    assert PureS3Path('foo//bar') == PureS3Path('foo/bar')
    assert PureS3Path('foo/./bar') == PureS3Path('foo/bar')
    assert PureS3Path('foo/../bar') == PureS3Path('bar')
    assert PureS3Path('../bar') == PureS3Path('../bar')
    assert PureS3Path('foo', '../bar') == PureS3Path('bar')


def test_operators():
    assert PureS3Path('/etc') / 'init.d' / 'apache2' == PureS3Path('/etc/init.d/apache2')
    assert '/usr' / PureS3Path('bin') == PureS3Path('/usr/bin')


def test_parts():
    assert PureS3Path('foo//bar').parts == ('foo', 'bar')
    assert PureS3Path('foo/./bar').parts == ('foo', 'bar')
    assert PureS3Path('foo/../bar').parts == ('bar',)
    assert PureS3Path('../bar').parts == ('..', 'bar')
    assert PureS3Path('foo', '../bar').parts == ('bar',)
    assert PureS3Path('/foo/bar').parts == ('/', 'foo', 'bar')


@pytest.mark.parametrize("path", ["/foo", "/foo/"])
def test_is_bucket_with_valid_bucket_paths(path):
    assert PureS3Path(path).is_bucket


@pytest.mark.parametrize("path", ["//foo", "foo/", "foo", "", "/foo/bar"])
def test_is_bucket_with_invalid_bucket_paths(path):
    assert not PureS3Path(path).is_bucket


def test_drive():
    assert PureS3Path('foo//bar').drive == ''
    assert PureS3Path('foo/./bar').drive == ''
    assert PureS3Path('foo/../bar').drive == ''
    assert PureS3Path('../bar').drive == ''
    assert PureS3Path('foo', '../bar').drive == ''
    assert PureS3Path('/foo/bar').drive == ''


def test_root():
    assert PureS3Path('foo//bar').root == ''
    assert PureS3Path('foo/./bar').root == ''
    assert PureS3Path('foo/../bar').root == ''
    assert PureS3Path('../bar').root == ''
    assert PureS3Path('foo', '../bar').root == ''
    assert PureS3Path('/foo/bar').root == '/'


def test_anchor():
    assert PureS3Path('foo//bar').anchor == ''
    assert PureS3Path('foo/./bar').anchor == ''
    assert PureS3Path('foo/../bar').anchor == ''
    assert PureS3Path('../bar').anchor == ''
    assert PureS3Path('foo', '../bar').anchor == ''
    assert PureS3Path('/foo/bar').anchor == '/'


def test_parents():
    assert tuple(PureS3Path('foo//bar').parents) == (PureS3Path('foo'), PureS3Path('.'))
    assert tuple(PureS3Path('foo/./bar').parents) == (PureS3Path('foo'), PureS3Path('.'))
    assert tuple(PureS3Path('foo/../bar').parents) == (PureS3Path('.'),)
    assert tuple(PureS3Path('../bar').parents) == (PureS3Path('..'), PureS3Path('.'))
    assert tuple(PureS3Path('foo', '../bar').parents) == (PureS3Path('.'),)
    assert tuple(PureS3Path('/foo/bar').parents) == (PureS3Path('/foo'), PureS3Path('/'))


def test_parent():
    assert PureS3Path('foo//bar').parent == PureS3Path('foo')
    assert PureS3Path('foo/./bar').parent == PureS3Path('foo')
    assert PureS3Path('foo/../bar').parent == PureS3Path('.')
    assert PureS3Path('../bar').parent == PureS3Path('..')
    assert PureS3Path('foo', '../bar').parent == PureS3Path('.')
    assert PureS3Path('/foo/bar').parent == PureS3Path('/foo')
    assert PureS3Path('.').parent == PureS3Path('.')
    assert PureS3Path('/').parent == PureS3Path('/')


def test_name():
    assert PureS3Path('my/library/setup.py').name == 'setup.py'


def test_suffix():
    assert PureS3Path('my/library/setup.py').suffix == '.py'
    assert PureS3Path('my/library.tar.gz').suffix == '.gz'
    assert PureS3Path('my/library').suffix == ''


def test_suffixes():
    assert PureS3Path('my/library.tar.gar').suffixes == ['.tar', '.gar']
    assert PureS3Path('my/library.tar.gz').suffixes == ['.tar', '.gz']
    assert PureS3Path('my/library').suffixes == []


def test_stem():
    assert PureS3Path('my/library.tar.gar').stem == 'library.tar'
    assert PureS3Path('my/library.tar').stem == 'library'
    assert PureS3Path('my/library').stem == 'library'


def test_uri():
    assert PureS3Path('/etc/passwd').as_uri() == 's3://etc/passwd'
    assert PureS3Path('/etc/init.d/apache2').as_uri() == 's3://etc/init.d/apache2'
    assert PureS3Path('/bucket/key').as_uri() == 's3://bucket/key'


def test_absolute():
    assert PureS3Path('/a/b').is_absolute()
    assert not PureS3Path('a/b').is_absolute()


def test_reserved():
    assert not PureS3Path('/a/b').is_reserved()
    assert not PureS3Path('a/b').is_reserved()


def test_joinpath():
    assert PureS3Path('/etc').joinpath('passwd') == PureS3Path('/etc/passwd')
    assert PureS3Path('/etc').joinpath(PureS3Path('passwd')) == PureS3Path('/etc/passwd')
    assert PureS3Path('/etc').joinpath('init.d', 'apache2') == PureS3Path('/etc/init.d/apache2')


def test_match():
    assert PureS3Path('a/b.py').match('*.py')
    assert PureS3Path('/a/b/c.py').match('b/*.py')
    assert not PureS3Path('/a/b/c.py').match('a/*.py')
    assert PureS3Path('/a.py').match('/*.py')
    assert not PureS3Path('a/b.py').match('/*.py')
    assert not PureS3Path('a/b.py').match('*.Py')


def test_relative_to():
    s3_path = PureS3Path('/etc/passwd')
    assert s3_path.relative_to('/') == PureS3Path('etc/passwd')
    assert s3_path.relative_to('/etc') == PureS3Path('passwd')
    with pytest.raises(ValueError):
        s3_path.relative_to('/usr')


def test_with_name():
    s3_path = PureS3Path('/Downloads/pathlib.tar.gz')
    assert s3_path.with_name('setup.py') == PureS3Path('/Downloads/setup.py')
    s3_path = PureS3Path('/')
    with pytest.raises(ValueError):
        s3_path.with_name('setup.py')


def test_with_suffix():
    s3_path = PureS3Path('/Downloads/pathlib.tar.gz')
    assert s3_path.with_suffix('.bz2') == PureS3Path('/Downloads/pathlib.tar.bz2')
    s3_path = PureS3Path('README')
    assert s3_path.with_suffix('.txt') == PureS3Path('README.txt')
    s3_path = PureS3Path('README.txt')
    assert s3_path.with_suffix('') == PureS3Path('README')
