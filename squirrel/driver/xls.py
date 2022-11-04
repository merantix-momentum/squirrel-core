from __future__ import annotations

from typing import TYPE_CHECKING

from squirrel.driver.data_frame import DataFrameDriver

if TYPE_CHECKING:
    import pandas as pd


class XlsDriver(DataFrameDriver):

    name = "xls"

    def __init__(self, **kwargs) -> None:
        """Initializes XlsDriver.

        Args:
            **kwargs: See DataFrameDriver.
        """
        super().__init__(**kwargs)
        if self.use_dask:
            raise ValueError("Dask does not support reading XLS files.")

    def read(self, **kwargs) -> pd.DataFrame:
        """Read Excel file using pandas."""
        import pandas as pd

        return pd.read_excel(self.path, **kwargs)
