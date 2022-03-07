import typing as t

from dask import dataframe

from squirrel.constants import URL
from squirrel.dataset.dataset import AbstractDataset
from squirrel.dataset.stream_dataset import Fetcher, StreamDataset


class CsvDataset(AbstractDataset):
    def __init__(self, path: URL, **kwargs):
        """Initialize CsvDataset.

        Args:
            path (URL): Url path to this dataset.
            **kwargs: Keyword arguments to be passed to the driver.
        """
        self.path = path
        self.kwargs = kwargs

    @staticmethod
    def get_stream(fetcher: Fetcher, cache_size: int, max_workers: int) -> StreamDataset:
        """Get stream csv dataset."""
        raise NotImplementedError

    def get_driver(self) -> t.Any:
        """Get csv driver."""
        return dataframe.read_csv(self.path, **self.kwargs)
