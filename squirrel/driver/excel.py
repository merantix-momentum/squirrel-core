from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.constants import URL
from squirrel.driver.data_frame import DataFrameDriver

if TYPE_CHECKING:
    import pandas as pd


class ExcelDriver(DataFrameDriver):

    name = "excel"

    def __init__(self, url: URL, **kwargs) -> None:
        """Driver to read Excel files into a DataFrame.

        Args:
            url (URL): URL to file. Prefix with a protocol like ``s3://`` or ``gs://`` to read from other filesystems.
                       For a full list of accepted types, refer to :py:func:`pandas.read_excel()`.
            **kwargs: See DataFrameDriver.
        """
        super().__init__(url, **kwargs)
        if self.engine == "dask":
            raise ValueError("Dask does not support reading Excel files.")

    def _read(self, **kwargs) -> pd.DataFrame:
        """Read Excel file using pandas."""
        import pandas as pd

        return pd.read_excel(self.url, **kwargs)
