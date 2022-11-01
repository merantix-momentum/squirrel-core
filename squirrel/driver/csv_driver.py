from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame_file_driver import DataFrameFileDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class CsvDriver(DataFrameFileDriver):

    name = "csv"

    def __init__(self, *args, **kwargs) -> None:
        """Initializes CsvDriver.

        Args:
            *args: See DataFrameFileDriver.
            **kwargs: See DataFrameFileDriver.
        """
        super().__init__(*args, **kwargs)

    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read CSV file using dask or pandas."""
        if self.use_dask:
            import dask.dataframe as dd

            return dd.read_csv(self.path, **kwargs)
        else:
            import pandas as pd

            return pd.read_csv(self.path, **kwargs)
