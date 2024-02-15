from enum import Enum
from functools import partial
from typing import Any, List
import ray
# import daft
import polars as pl

from squirrel.driver.driver import IterDriver
from squirrel.driver.store import StoreDriver
from squirrel.iterstream import IterableSource
from squirrel.iterstream.base import Composable
from squirrel.store.parquet_store import ParquetStore
from squirrel.store.store import AbstractStore


class Engine(Enum):
    daft = 'daft'
    ray = 'ray'


# class Query:

#     def __init__(self, urls: List[str]):
#         self.urls = urls
    
#     def read(self):
#         c = pl.SQLContext()
#         aa = pl.scan_parquet(self.urls[0] + "/*")
#         bb = pl.scan_parquet(self.urls[1] + "/*")
#         c.register("aa", aa)
#         c.register("bb", bb)
#         sql_res = c.execute(
#                 """
#                 SELECT * FROM aa
#                 INNER JOIN bb
#                 ON aa.id = bb.id
#             """
#             )
#         print(f"sql_res  :::::: {sql_res}")
#         return sql_res.collect()


class Transformation:
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
    
    def func(self) -> Any:
        return partial(self.f, *self.args, **self.kwargs)


class GenericDriver(IterDriver):

    name: str = "generic_driver"

    def __init__(
            self,
            url,
            # query: Query = None,
            transformations: List[Transformation] = ()
        ):
        self.url = url
        # self.query = query,
        self.transformations = transformations
        self._readers = {
            "parquet": ray.data.read_parquet,
            "csv": ray.data.read_csv,
        }
        # print(self._readers)

    def get_iter(
            self,
            flatten: bool = False,
        ):
        """
        ray
        """
        if self.query:
            data = self.query.read()
            ds = ray.data.from_items(data)
        else:
            ds = self.dataset()
        
        it = IterableSource(ds.iter_batches())
        if flatten:
            it = it.flatten()
        return it

    def dataset(self):
        ds = self._get_reader(self.file_format())(paths=self.url)
        for trans in self.transformations:
            ds = ds.map_batches(trans.func())
        return ds
    
    def schema(self):
        return self.dataset().schema()

    def file_format(self) -> str:
        from squirrel.fsspec.fs import get_fs_from_url
        fs = get_fs_from_url(self.url)
        paths = fs.ls(self.url)
        _ff = paths[0].split(".")[-1]
        return _ff

    def _get_reader(self, file_format):
        return self._readers[file_format]


class StreamingParquetDriver(StoreDriver):
    def __init__(self, url: str, serializer=None):
        """
        backend: pyarrow or "deltalake"
        """
        super().__init__(url=url, serializer=None)

    @property
    def store(self) -> AbstractStore:
        return ParquetStore(self.url, **self.storage_options)
    
    @property
    def ray(self) -> "ray.data.Dataset":
        import ray
        return ray.data.read_parquet(self.url)
    
    @property
    def deltatable(self) -> "deltalake.DeltaTable":
        """If the underlying store is a deltalake table, this property return a deltalake.DeltaTable object. If this in not the case, it will fail"""
        from deltalake import DeltaTable
        # so = {} if self.storage_options is None else self.storage_options
        return DeltaTable(table_uri=self.url)