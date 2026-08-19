import os

import pytest

from kitchen.remote import LocalRemote


@pytest.fixture
def remote(tmp_path):
    return LocalRemote(root=str(tmp_path / "root"))


def test_home_is_per_host(remote):
    assert remote.home("a") != remote.home("b")
    assert os.path.isdir(remote.home("a"))


def test_ssh_runs_with_fake_home(remote):
    result = remote.ssh("a", "echo $HOME")
    assert result.stdout.strip() == remote.home("a")


def test_upload_expands_tilde_and_relative(remote, tmp_path):
    src = tmp_path / "f.txt"
    src.write_text("hello")
    remote.scp_upload(str(src), "a", "~/sub/f.txt")
    assert (tmp_path / "root" / "a" / "sub" / "f.txt").read_text() == "hello"
    remote.scp_upload(str(src), "a", "rel.txt")
    assert (tmp_path / "root" / "a" / "rel.txt").exists()


def test_upload_trailing_slash_means_into_dir(remote, tmp_path):
    src = tmp_path / "f.txt"
    src.write_text("hello")
    remote.scp_upload(str(src), "a", "~/dir/")
    assert (tmp_path / "root" / "a" / "dir" / "f.txt").exists()


def test_download_missing_raises(remote, tmp_path):
    with pytest.raises(RuntimeError, match="no such file"):
        remote.scp_download("a", "~/nope", str(tmp_path / "out"))


def test_ips_are_loopback(remote):
    assert remote.get_ip("a") == "127.0.0.1"
    assert remote.get_all_ips(["a", "b"]) == {"a": "127.0.0.1", "b": "127.0.0.1"}
