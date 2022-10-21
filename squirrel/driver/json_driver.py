from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame_file_driver import DataFrameFileDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class JsonDriver(DataFrameFileDriver):

    name = "json"

    def __init__(self, **kwargs) -> None:
        """Initializes JsonDriver.

        Args:
            **kwargs: See DataFrameFileDriver.
        """
        super().__init__(**kwargs)

    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read JSON file using dask or pandas."""
        if self.use_dask:
            import dask.dataframe as dd

            return dd.read_json(self.path, **kwargs)
        else:
            import pandas as pd

            return pd.read_json(self.path, **kwargs)
