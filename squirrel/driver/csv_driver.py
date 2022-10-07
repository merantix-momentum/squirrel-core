from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterable

from squirrel.driver.driver import DataFrameDriver
from squirrel.driver.file_driver import FileDriver
from squirrel.iterstream import Composable, IterableSource

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class CsvDriver(FileDriver, DataFrameDriver):

    name = "csv"

    def __init__(
        self,
        path: str,
        use_dask: bool = True,
        df_hooks: Iterable[Callable] | None = None,
        read_csv_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        """Initializes CsvDriver.

        Args:
            path (str): Path to a .csv file.
            use_dask (bool): Whether to anychronously load the dataframe with dask.
            df_hooks (Iterable[Callable], optional): Preprocessing hooks to execute on the dataframe.
                The first hook must accept a dask.dataframe.DataFrame or pandas.Dataframe in accordance with use_dask.
            read_csv_kwargs: Arguments passed to all instances of :py:func:`dask.dataframe.read_csv` or
                :py:func:`pandas.dataframe.read_csv` used in class methods.
            **kwargs: Keyword arguments passed to the super class initializer.
        """
        super().__init__(path, **kwargs)
        self.use_dask = use_dask
        self.df_hooks = df_hooks or []
        self.read_csv_kwargs = read_csv_kwargs or {}

    def get_df(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Returns the data in the .csv file as a Dask DataFrame.

        Args:
            **kwargs: Keyword arguments passed to :py:func:`dask.dataframe.read_csv` or
                :py:func:`pandas.dataframe.read_csv` to read the .csv file. Takes precedence over arguments specified at
                class initialization.

        Returns:
            (dask.dataframe.DataFrame | pandas.DataFrame) Dask or Pandas DataFrame constructed from the .csv file.
        """
        # Join kwargs for read_csv where kwargs takes precedence over self.read_csv_kwargs
        read_csv_kwargs = {**self.read_csv_kwargs, **kwargs}
        if self.use_dask:
            import dask.dataframe as dd

            result = dd.read_csv(self.path, **read_csv_kwargs)
        else:
            import pandas as pd

            result = pd.read_csv(self.path, **read_csv_kwargs)
        for hook in self.df_hooks:
            result = hook(result)

        return result

    def get_iter(self, itertuples_kwargs: dict | None = None, read_csv_kwargs: dict | None = None) -> Composable:
        """Returns an iterator over rows.

        Note that first the csv file is read into a DataFrame and then :py:meth:`df.itertuples` is called.

        Args:
            itertuples_kwargs: Keyword arguments to be passed to :py:meth:`dask.dataframe.DataFrame.itertuples`.
                or :py:func:`pandas.dataframe.DataFrame.itertuples`
            read_csv_kwargs: Keyword arguments to be passed to :py:func:`dask.dataframe.read_csv` or
                :py:func:`pandas.dataframe.read_csv`. Takes precedence over arguments specified at class initialization.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the rows of the data frame as namedtuples.
        """
        # Join kwargs for read_csv where read_csv_kwargs takes precedence over self.read_csv_kwargs
        read_csv_kwargs = read_csv_kwargs or {}
        read_csv_kwargs = {**self.read_csv_kwargs, **read_csv_kwargs}

        if itertuples_kwargs is None:
            itertuples_kwargs = {}
        if read_csv_kwargs is None:
            read_csv_kwargs = {}
        return IterableSource(self.get_df(**read_csv_kwargs).itertuples(**itertuples_kwargs))
