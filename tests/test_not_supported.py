import pytest
from s3path import S3Path


def test_not_supported(monkeypatch):
    monkeypatch.setattr(S3Path._flavour, 'is_supported', False)
    error_message = f'cannot instantiate {S3Path.__name__} on your system'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path()


def test_cwd():
    error_message = f'{S3Path.__name__}.cwd() is unsupported on S3 service'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.cwd()


def test_home():
    error_message = f'{S3Path.__name__}.home() is unsupported on S3 service'
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.home()


def test_chmod():
    error_message = f'{S3Path.__name__}.chmod() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.chmod(0o666)


def test_group():
    error_message = f'{S3Path.__name__}.group() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.group()


def test_is_mount():
    error_message = f'{S3Path.__name__}.is_mount() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_mount()


def test_is_symlink():
    error_message = f'{S3Path.__name__}.is_symlink() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_symlink()


def test_is_socket():
    error_message = f'{S3Path.__name__}.is_socket() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_socket()


def test_is_fifo():
    error_message = f'{S3Path.__name__}.is_fifo() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_fifo()


def test_is_block_device():
    error_message = f'{S3Path.__name__}.is_block_device() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_block_device()


def test_is_char_device():
    error_message = f'{S3Path.__name__}.is_char_device() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_char_device()


def test_lstat():
    error_message = f'{S3Path.__name__}.lstat() is unsupported on S3 service'
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.lstat()
