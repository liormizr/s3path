import pytest
from s3path import S3Path


def test_not_supported(monkeypatch):
    monkeypatch.setattr(S3Path._flavour, 'is_supported', False)
    error_message = 'cannot instantiate {} on your system'.format(S3Path.__name__)
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path()


def test_cwd():
    error_message = '{}.cwd() is unsupported on S3 service'.format(S3Path.__name__)
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.cwd()


def test_home():
    error_message = '{}.home() is unsupported on S3 service'.format(S3Path.__name__)
    with pytest.raises(NotImplementedError, message=error_message):
        S3Path.home()


def test_chmod():
    error_message = '{}.chmod() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.chmod(0o666)


def test_lchmod():
    error_message = '{}.lchmod() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.lchmod(0o666)


def test_group():
    error_message = '{}.group() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.group()


def test_is_mount():
    error_message = '{S3Path.__name__}.is_mount() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_mount()


def test_is_symlink():
    error_message = '{}.is_symlink() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_symlink()


def test_is_socket():
    error_message = '{}.is_socket() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_socket()


def test_is_fifo():
    error_message = '{}.is_fifo() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_fifo()


def test_is_block_device():
    error_message = '{}.is_block_device() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_block_device()


def test_is_char_device():
    error_message = '{}.is_char_device() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.is_char_device()


def test_lstat():
    error_message = '{}.lstat() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.lstat()


def test_mkdir():
    error_message = '{}.mkdir() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.mkdir()


def test_resolve():
    error_message = '{}.resolve() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.resolve()


def test_unlink():
    error_message = '{}.unlink() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.unlink()


def test_symlink_to():
    error_message = '{}.symlink_to() is unsupported on S3 service'.format(S3Path.__name__)
    path = S3Path('/fake-bucket/fake-key')
    with pytest.raises(NotImplementedError, message=error_message):
        path.symlink_to('file_name')
