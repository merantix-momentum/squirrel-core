from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Iterable

from squirrel.driver.driver import DataFrameDriver
from squirrel.driver.file import FileDriver
from squirrel.iterstream import Composable, IterableSource

if TYPE_CHECKING:
    import pandas as pd
    from dask.dataframe import DataFrame


class DataFrameFileDriver(FileDriver, DataFrameDriver, metaclass=ABCMeta):
    def __init__(
        self,
        path: str,
        storage_options: dict[str, Any] | None = None,
        use_dask: bool = True,
        df_hooks: Iterable[Callable] | None = None,
        read_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        """Abstract DataFrameFileDriver.

        This defines a common interface for all driver using different read methods to read the dataframe such as
        from .csv, .xls, .parqet etc. These derived drivers have to only specify the read() method.

        Args:
            path (str): Path to a file.
            storage_options (Optional[Dict[str, Any]]): a dict with keyword arguments passed to file system initializer
            use_dask (bool): Whether to anychronously load the dataframe with dask.
            df_hooks (Iterable[Callable], optional): Preprocessing hooks to execute on the dataframe.
                The first hook must accept a dask.dataframe.DataFrame or pandas.Dataframe in accordance with use_dask.
            read_kwargs: Arguments passed to all read methods of the derived driver.
            **kwargs: Keyword arguments passed to the FileDriver class initializer.
        """
        super().__init__(path, storage_options, **kwargs)
        self.use_dask = use_dask
        self.df_hooks = df_hooks or []
        self.read_kwargs = read_kwargs or {}

    @abstractmethod
    def read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read DataFrame from file."""
        raise NotImplementedError("This needs to be implemented by the derived class.")

    def _read(self, **kwargs) -> DataFrame | pd.DataFrame:
        """Read DataFrame.
        Wraps the underlying read() to handle read arguments and storage options.
        """
        # Join kwargs for read()
        # Passed kwargs takes precedence over self.read_kwargs and self.storage_kwargs
        storage_kwargs = {"storage_options": self.storage_options} if self.storage_options else {}
        read_kwargs = {**self.read_kwargs, **storage_kwargs, **kwargs}
        return self.read(**read_kwargs)

    def get_df(self, **read_kwargs) -> DataFrame | pd.DataFrame:
        """Returns the data as a DataFrame.

        Args:
            **read_kwargs: Keyword arguments to be passed to read().
                      Takes precedence over arguments specified at class initialization.

        Returns:
            (dask.dataframe.DataFrame | pandas.DataFrame) Dask or Pandas DataFrame constructed from the .csv file.
        """
        result = self._read(**read_kwargs)

        for hook in self.df_hooks:
            result = hook(result)

        return result

    def get_iter(self, itertuples_kwargs: dict | None = None, read_kwargs: dict | None = None) -> Composable:
        """Returns an iterator over DataFrame rows.

        Note that first the csv file is read into a DataFrame and then :py:meth:`df.itertuples` is called.

        Args:
            itertuples_kwargs: Keyword arguments to be passed to :py:meth:`dask.dataframe.DataFrame.itertuples`.
                or :py:func:`pandas.dataframe.DataFrame.itertuples`
            read_kwargs: Keyword arguments to be passed to read().
                         Takes precedence over arguments specified at class initialization.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the rows of the data frame as namedtuples.
        """
        if itertuples_kwargs is None:
            itertuples_kwargs = {}
        if read_kwargs is None:
            read_kwargs = {}
        return IterableSource(self.get_df(**read_kwargs).itertuples(**itertuples_kwargs))
