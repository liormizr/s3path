import pytest
from gcspath import GCSPath


def test_not_supported(monkeypatch):
    monkeypatch.setattr(GCSPath._flavour, "is_supported", False)
    with pytest.raises(NotImplementedError):
        GCSPath()


def test_cwd():
    with pytest.raises(NotImplementedError):
        GCSPath.cwd()


def test_home():
    with pytest.raises(NotImplementedError):
        GCSPath.home()


def test_chmod():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.chmod(0o666)


def test_lchmod():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.lchmod(0o666)


def test_group():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.group()


def test_is_mount():
    assert not GCSPath("/fake-bucket/fake-key").is_mount()


def test_is_symlink():
    assert not GCSPath("/fake-bucket/fake-key").is_symlink()


def test_is_socket():
    assert not GCSPath("/fake-bucket/fake-key").is_socket()


def test_is_fifo():
    assert not GCSPath("/fake-bucket/fake-key").is_fifo()


def test_is_block_device():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.is_block_device()


def test_is_char_device():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.is_char_device()


def test_lstat():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.lstat()


def test_resolve():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.resolve()


def test_unlink():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.unlink()


def test_symlink_to():
    path = GCSPath("/fake-bucket/fake-key")
    with pytest.raises(NotImplementedError):
        path.symlink_to("file_name")
