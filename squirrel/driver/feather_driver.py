from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame_file_driver import DataFrameFileDriver

if TYPE_CHECKING:
    import pandas as pd


class FeatherDriver(DataFrameFileDriver):

    name = "feather"

    def __init__(self, *args, **kwargs) -> None:
        """Initializes ParquetDriver.

        Args:
            *args: See DataFrameFileDriver.
            **kwargs: See DataFrameFileDriver.
        """
        super().__init__(*args, **kwargs)
        if self.use_dask:
            raise ValueError("Dask does not support reading feather files.")

    def read(self, **kwargs) -> pd.DataFrame:
        """Read Parquet file using pandas."""
        import pandas as pd

        return pd.read_feather(self.path, **kwargs)
