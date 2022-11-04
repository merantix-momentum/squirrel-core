from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame_file import DataFrameFileDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class JsonDriver(DataFrameFileDriver):

    name = "json"

    def __init__(self, *args, **kwargs) -> None:
        """Initializes JsonDriver.

        Args:
            *args: See DataFrameFileDriver.
            **kwargs: See DataFrameFileDriver.
        """
        super().__init__(*args, **kwargs)

    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read JSON file using dask or pandas."""
        if self.use_dask:
            import dask.dataframe as dd

            return dd.read_json(self.path, **kwargs)
        else:
            import pandas as pd

            return pd.read_json(self.path, **kwargs)
