from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame import DataFrameDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class ParquetDriver(DataFrameDriver):

    name = "parquet"

    def __init__(self, *args, **kwargs) -> None:
        """Initializes ParquetDriver.

        Args:
            *args: See DataFrameDriver.
            **kwargs: See DataFrameDriver.
        """
        super().__init__(*args, **kwargs)

    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read Parquet file using pandas."""
        if self.use_dask:
            import dask.dataframe as dd

            return dd.read_parquet(self.path, **kwargs)
        else:
            import pandas as pd

            return pd.read_parquet(self.path, **kwargs)
