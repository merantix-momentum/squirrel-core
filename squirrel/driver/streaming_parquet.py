from typing import Any, Dict, Optional

from squirrel.driver.store import StoreDriver
from squirrel.iterstream import Composable
from squirrel.iterstream.source import IterableSource
from squirrel.serialization.parquet import DeltalakeSerializer, ParquetSerializer, PolarsSerializer
from squirrel.store import ParquetStore
from squirrel.store.parquet_store import DeltalakeStore

__all__ = ["StreamingParquetDriver", "DeltalakeDriver", "PolardParquetDriver"]


class StreamingParquetDriver(StoreDriver):

    name = "streaming_parquet"

    def __init__(self, url: str, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, serializer=ParquetSerializer(), storage_options=storage_options, **kwargs)

    @property
    def store(self) -> ParquetStore:
        """Return squirrel.store.ParquetStore"""
        return ParquetStore(self.url, **self.storage_options)

    def ray(self, **kwargs) -> "ray.data.Dataset":
        """Returns ray.data.Dataset"""
        import ray

        return ray.data.read_parquet(self.url, **kwargs)


class DeltalakeDriver(StreamingParquetDriver):
    name = "deltalake_parquet"

    def __init__(self, url: str, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, storage_options=storage_options, **kwargs)
        self.serializer = DeltalakeSerializer()

    @property
    def store(self) -> DeltalakeStore:
        """Return squirrel.store.DeltalakeStore"""
        return DeltalakeStore(self.url, **self.storage_options)

    def deltatable(self, **kwargs) -> "deltalake.DeltaTable":  # noqa F821
        """Return a deltalake.DeltaTable"""
        from deltalake import DeltaTable

        return DeltaTable(table_uri=self.url, **kwargs)


class PolardParquetDriver(StreamingParquetDriver):
    name = "polars_parquet"

    def __init__(self, url: str, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, storage_options=storage_options, **kwargs)
        self.serializer = PolarsSerializer()

    def lazy_frame(self, **scan_kwargs) -> "polars.LazyFrame":  # noqa F821
        """
        Returns a polard.LazyFrame object

        Args:
            - scan_kwargs: kwargs passed to Polars.scan_parquet()

        Note: the `source` argument of scan_parquet is set to self.url + "/*", which assumes non-partitioned
        parquet
        """
        import polars as pl

        return pl.scan_parquet(source=self.url + "/*", **scan_kwargs)

    def query(
        self,
        query: str = "SELECT * FROM frame LIMIT 2",
        scan_kwargs: Optional[Dict] = None,
        sql_context_kwargs: Optional[Dict] = None,
    ) -> "polars.LazyFrame":  # noqa F821
        """Run the query against the parquet dataset and return a polard.LazyFrame object"""
        import polars as pl

        if scan_kwargs is None:
            scan_kwargs = {}
        if sql_context_kwargs is None:
            sql_context_kwargs = {}

        res = pl.SQLContext(frame=self.lazy_frame(**scan_kwargs), **sql_context_kwargs).execute(query)
        return res

    def get_iter(
        self,
        query: Optional[str] = None,
        streaming: bool = True,
        scan_kwargs: Optional[Dict] = None,
        sql_context_kwargs: Optional[Dict] = None,
    ) -> Composable:
        """A Composable based on the polars.LazyFrame.  # noqaD417

        Args:
            - query (str): apply this query before fetching the data. The general form should be
                `"SELECT * FROM frame`.
            - streaming (bool): whether to stream data using Polars or not. Some opperations might not be
                compatible with streaming. Please consult the Polars documentation for more info.
            - scan_kwargs (Optional[Dict]): kwargs passed to `polars.scan_parquet`.
            - sql_context_kwargs (Optional[Dict]): kwargs passed to `polars.SQLContext`. Only applicable if
                `query` is not None.

        """
        scan_kwargs = {} if scan_kwargs is None else scan_kwargs
        if sql_context_kwargs is None:
            sql_context_kwargs = {}

        if query is not None:
            it = self.query(q=query, scan_kwargs=scan_kwargs, sql_context_kwargs=sql_context_kwargs)
        else:
            it = self.lazy_frame(**scan_kwargs)
        return IterableSource(it.collect(streaming=streaming).iter_rows(named=True))
