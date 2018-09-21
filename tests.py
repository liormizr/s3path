import os
import pytest
from pathlib import Path
from s3path import PureS3Path


#Basic PurePath operations

def test_repr():
    assert repr(PureS3Path('setup.py')) == "PureS3Path('setup.py')"
    assert str(PureS3Path('setup.py')) == 'setup.py'
    assert bytes(PureS3Path('setup.py')) == b'setup.py'
    assert os.fspath(PureS3Path('/usr/bin')) == '/usr/bin'


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

