from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.constants import URL
from squirrel.driver.data_frame import DataFrameDriver

if TYPE_CHECKING:
    import pandas as pd


class FeatherDriver(DataFrameDriver):

    name = "feather"

    def __init__(self, url: URL, *args, **kwargs) -> None:
        """Driver to read Feather files into a DataFrame.

        Args:
            url (URL): URL to file. Prefix with a protocol like ``s3://`` or ``gs://`` to read from other filesystems.
                       For a full list of accepted types, refer to :py:func:`pandas.read_feather()`.
            *args: See DataFrameDriver.
            **kwargs: See DataFrameDriver.
        """
        super().__init__(url, *args, **kwargs)
        if self.engine == "dask":
            raise ValueError("Dask does not support reading feather files.")

    def _read(self, **kwargs) -> pd.DataFrame:
        """Read Parquet file using pandas."""
        import pandas as pd

        return pd.read_feather(self.url, **kwargs)
