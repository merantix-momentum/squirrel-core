from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from squirrel.driver.driver import DataFrameDriver
from squirrel.driver.file_driver import FileDriver
from squirrel.iterstream import Composable, IterableSource

if TYPE_CHECKING:
    from dask.dataframe import DataFrame


class CsvDriver(FileDriver, DataFrameDriver):

    name = "csv"

    def __init__(self, path: str, **kwargs) -> None:
        """Initializes CsvDriver.

        Args:
            path (str): Path to a .csv file.
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        super().__init__(path, **kwargs)

    def get_df(self, **kwargs) -> DataFrame:
        """Returns the data in the .csv file as a Dask DataFrame.

        Args:
            **kwargs: Keyword arguments passed to :py:func:`dask.dataframe.read_csv` to read the .csv file.

        Returns:
            (dask.dataframe.DataFrame) Dask DataFrame constructed from the .csv file.
        """
        import dask.dataframe as dd

        return dd.read_csv(self.path, **kwargs)

    def get_iter(self, itertuples_kwargs: Optional[Dict] = None, read_csv_kwargs: Optional[Dict] = None) -> Composable:
        """Returns an iterator over rows.

        Note that first the csv file is read into a DataFrame and then :py:meth:`df.itertuples` is called.

        Args:
            itertuples_kwargs: Keyword arguments to be passed to :py:meth:`dask.dataframe.DataFrame.itertuples`.
            read_csv_kwargs: Keyword arguments to be passed to :py:func:`dask.dataframe.read_csv`.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the rows of the data frame as namedtuples.
        """
        if itertuples_kwargs is None:
            itertuples_kwargs = dict()
        if read_csv_kwargs is None:
            read_csv_kwargs = dict()
        return IterableSource(self.get_df(**read_csv_kwargs).itertuples(**itertuples_kwargs))
