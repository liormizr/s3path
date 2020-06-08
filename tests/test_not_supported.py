import pytest
from s3path import S3Path


def test_not_supported(monkeypatch):
    monkeypatch.setattr(S3Path._flavour, 'is_supported', False)
    with pytest.raises(NotImplementedError):
        S3Path()


def test_cwd():
    with pytest.raises(NotImplementedError):
        S3Path.cwd()


def test_home():
    with pytest.raises(NotImplementedError):
        S3Path.home()


def test_chmod():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.chmod(0o666)


def test_lchmod():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.lchmod(0o666)


def test_group():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.group()


def test_is_mount():
    assert not S3Path('/fake-bucket/fake-key').is_mount()


def test_is_symlink():
    assert not S3Path('/fake-bucket/fake-key').is_symlink()


def test_is_socket():
    assert not S3Path('/fake-bucket/fake-key').is_socket()


def test_is_fifo():
    assert not S3Path('/fake-bucket/fake-key').is_fifo()


def test_is_block_device():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.is_block_device()


def test_is_char_device():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.is_char_device()


def test_lstat():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.lstat()


def test_resolve():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.resolve()


def test_symlink_to():
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError):
        path.symlink_to('file_name')
