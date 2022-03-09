import gcsfs
from gcsfs import GCSFileSystem
from gcsfs.retry import HttpError, is_retriable

_original_retry = is_retriable


def custom_retry(exception: Exception) -> bool:
    """
    We have intermittent failures in reading data and on retry it works, it could be because of this issue
    https://github.com/dask/gcsfs/issues/290 hence monkey patching to include 400 as retriable
    other relevant issues: https://github.com/dask/gcsfs/issues/316, https://github.com/dask/gcsfs/issues/327,
    https://github.com/dask/gcsfs/issues/323
    """
    # Google Cloud occasionally throws Bad Requests (i.e. 400) for no apparent reason.
    if isinstance(exception, HttpError) and exception.code in (400, "400"):
        return True
    return _original_retry(exception)


class CustomGCSFileSystem(GCSFileSystem):
    """
    Monkey patch GCSFileSystem with a :py:func:`custom_retry` function that adds http error code 400 to be retriable.
    Other error codes are already included in :py:func:`gcsfs.utils.is_retriable`.
    """

    def __init__(self, token: str = "google_default", access: str = "read_write", **kwargs):
        """Overwrite gcsfs.utils.is_retriable"""
        self.token = token
        self.access = access
        gcsfs.retry.is_retriable = custom_retry
        super().__init__(**kwargs)
