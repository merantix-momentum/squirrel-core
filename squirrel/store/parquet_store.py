import typing as t

import os.path as osp
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.iterstream.source import FilePathGenerator

from squirrel.store.filesystem import FilesystemStore


class ParquetStore(FilesystemStore):

    def __init__(self, url: str, serializer=None, **storage_options):
        super().__init__(url=url, serializer=serializer, storage_options=storage_options)


    def get(self, key: str, mode: str = "rb", **open_kwargs) -> t.Any:
        # import pyarrow.dataset as ds
        import pyarrow.parquet as pq
        fs = get_fs_from_url(self.url)
        dataset = pq.ParquetDataset(self.url + f"/{key}", filesystem=fs) # filesystem=fs
        return dataset.read().to_pylist()


    def set(self, value: t.Any, key: t.Optional[str] = None, mode: str = "wb", **open_kwargs) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.Table.from_pylist(value)
        fs = get_fs_from_url(self.url)
        pq.write_to_dataset(table, root_path=self.url, filesystem=fs)

    def keys(self, nested: bool = True, **kwargs) -> t.Iterator[str]:

        storage_options = self.storage_options.copy()
        storage_options.update(kwargs)
        
        fp_gen = FilePathGenerator(url=self.url, nested=True, regex_filter="_delta_log", storage_options=self.storage_options)  # regex_filter="*_delta_log*"
        for path in fp_gen:
            yield osp.relpath(path, start=self.url)