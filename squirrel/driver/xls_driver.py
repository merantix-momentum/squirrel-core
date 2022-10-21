from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame_file_driver import DataFrameFileDriver

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class XlsDriver(DataFrameFileDriver):

    name = "xls"

    def __init__(self, **kwargs) -> None:
        """Initializes XlsDriver.

        Args:
            **kwargs: See DataFrameFileDriver.
        """
        super().__init__(**kwargs)
        if self.use_dask:
            raise ValueError("Dask does not support reading XLS files.")

    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read Excel file using pandas."""
        import pandas as pd

        return pd.read_excel(self.path, **kwargs)
