import ray

from squirrel.driver.store import StoreDriver
from squirrel.store import ParquetStore


class StreamingParquetDriver(StoreDriver):
    def __init__(self, url: str):
        """Driver to stream data from a parquet dataset."""
        super().__init__(url=url, serializer=None)

    @property
    def store(self) -> ParquetStore:
        """Return squirrel.store.ParquetStore"""
        return ParquetStore(self.url, **self.storage_options)

    @property
    def ray(self) -> "ray.data.Dataset":
        """Returns ray.data.Dataset"""
        import ray

        return ray.data.read_parquet(self.url)

    @property
    def deltatable(self) -> "deltalake.DeltaTable":
        """If the underlying store is a deltalake table, this property return a deltalake.DeltaTable object. If this in not the case, it will fail"""
        from deltalake import DeltaTable

        # so = {} if self.storage_options is None else self.storage_options
        return DeltaTable(table_uri=self.url)
