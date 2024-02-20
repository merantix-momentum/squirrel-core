from typing import Any, Dict, Optional

from squirrel.driver.store import StoreDriver
from squirrel.iterstream import Composable
from squirrel.iterstream.source import IterableSource
from squirrel.serialization.parquet import DeltalakeSerializer, ParquetSerializer, PolarsSerializer
from squirrel.store import ParquetStore

__all__ = ["StreamingParquetDriver", "DeltalakeDriver", "PolardParquetDriver"]


class StreamingParquetDriver(StoreDriver):

    name = "streaming_parquet"

    def __init__(self, url: str, serializer=None, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, serializer=ParquetSerializer, storage_options=storage_options, **kwargs)

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

    def __init__(self, url: str, serializer=None, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, serializer=DeltalakeSerializer, storage_options=storage_options, **kwargs)

    def deltatable(self, **kwargs) -> "deltalake.DeltaTable":  # noqa F821
        """
        If the underlying store is a deltalake table, this property return a deltalake.DeltaTable object.
        If this in not the case, it will fail
        """
        from deltalake import DeltaTable

        return DeltaTable(table_uri=self.url, **kwargs)


class PolardParquetDriver(StreamingParquetDriver):
    name = "polars_parquet"

    def __init__(self, url: str, serializer=None, storage_options: Optional[Dict[str, Any]] = None, **kwargs):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, serializer=PolarsSerializer, storage_options=storage_options, **kwargs)
        # self.flavour = flavour  # TODO: introduce deltalake

    def lazy_frame(self):
        """Returns a polard.LazyFrame object"""
        import polars as pl

        return pl.scan_parquet(source=self.url + "/*")

    def query(self, query: str = "SELECT * FROM frame LIMIT 2"):
        """Run the query against the parquet dataset and return a polard.LazyFrame object"""
        import polars as pl

        res = pl.SQLContext(frame=self.lazy_frame()).execute(query)
        return res

    def get_iter(self, query: Optional[str] = None, streaming: bool = True, **kwargs) -> Composable:
        """
        A Composable based on the polars.LazyFrame

        Args:
            - query (str): apply this query before fetching the data.
            - streaming (bool): whether to stream data using Polars or not. Some opperations might not be
                compatible with streaming. Please consult the Polars documentation for more info.
        """
        if query is not None:
            it = self.query(q=query)
        else:
            it = self.lazy_frame()
        return IterableSource(it.collect(streaming=streaming).iter_rows(named=True))
