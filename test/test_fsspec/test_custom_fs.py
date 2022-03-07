import fsspec
import pytest

from squirrel.constants import FILESYSTEM, URL
from squirrel.fsspec.custom_gcsfs import CustomGCSFileSystem
from squirrel.fsspec.fs import get_fs_from_url


@pytest.fixture
def fs(test_gcs_url: URL) -> FILESYSTEM:
    """Return an instance of custom gcsfs."""
    return get_fs_from_url(test_gcs_url)


def test_make_connection(fs: FILESYSTEM) -> None:
    """Test connection to custom gcsfs by gcs.ls command."""
    fs.ls("gs://mx-labs-squirrel-git-test")


def test_simple_upload(fs: FILESYSTEM, test_gcs_url: URL) -> None:
    """Test a simple upload case in gcs."""
    file = f"{test_gcs_url}/test_file"
    with fs.open(file, "wb", content_type="text/plain") as f:
        f.write(b"random word")
    with fs.open(file, "wb") as f:
        f.write(b"random word")
    assert fs.cat(file) == b"random word"


def test_fsspec_returns_custom_gcsfs() -> None:
    """Tests that fsspec returns our custom filesystem for gs:// protocol."""
    assert isinstance(fsspec.filesystem("gs"), CustomGCSFileSystem)
