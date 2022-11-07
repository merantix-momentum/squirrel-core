from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.constants import URL
from squirrel.driver.data_frame import DataFrameDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class CsvDriver(DataFrameDriver):

    name = "csv"

    def __init__(self, url: URL, *args, **kwargs) -> None:
        """Driver to read CSV files into a DataFrame.

        Args:
            url (URL): URL to file. Prefix with a protocol like ``s3://`` or ``gs://`` to read from other filesystems.
                       For a full list of accepted types, refer to :py:func:`pandas.read_csv()` or
                       :py:func:`dask.dataframe.read_csv()`.
            *args: See DataFrameDriver.
            **kwargs: See DataFrameDriver.
        """
        super().__init__(url, *args, **kwargs)

    def _read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read CSV file using dask or pandas."""
        if self.engine == "dask":
            import dask.dataframe as dd

            return dd.read_csv(self.url, **kwargs)
        else:
            import pandas as pd

            return pd.read_csv(self.url, **kwargs)
