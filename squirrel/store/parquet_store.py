import typing as t

from squirrel.fsspec.fs import get_fs_from_url
from squirrel.store.filesystem import FilesystemStore


class ParquetStore(FilesystemStore):
    def __init__(self, url: str, **storage_options):
        """Store that uses pyarrow to read from / write to the dataset"""
        super().__init__(url=url, serializer=None, storage_options=storage_options)

    def get(self, key: str, dataset_kwargs: t.Dict | None = None, **open_kwargs) -> t.Any:
        """Return the item with the given key.

        Args:
            key (str): Key corresponding to the item to retrieve.
            dataset_kwargs: Keyword arguments that will be forwarded to the `pyarrow.parquet.ParquetDataset`.
            **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.

        Return:
            (Any) Item with the given key.
        """
        if dataset_kwargs is None:
            dataset_kwargs = {}

        import pyarrow.parquet as pq

        fs = get_fs_from_url(self.url, **open_kwargs)
        dataset = pq.ParquetDataset(self.url + f"/{key}", filesystem=fs, **dataset_kwargs)
        return dataset.read().to_pylist()

    def set(self, value: t.Any, **open_kwargs) -> None:
        """Persists an item with the given key.

        Args:
            value (Any): Item to be persisted.
            dataset_kwargs: Keyword arguments that will be forwarded to the `pyarrow.parquet.write_to_dataset`.
            **open_kwargs: Keyword arguments that will be forwarded to the filesystem object when opening the file.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(value)
        fs = get_fs_from_url(self.url)
        pq.write_to_dataset(table, root_path=self.url, filesystem=fs)

    def keys(self, nested: bool = True, **kwargs) -> t.Iterator[str]:
        """Yields all paths in the store, relative to the root directory.

        Paths are generated using :py:class:`squirrel.iterstream.source.FilePathGenerator`.

        Args:
            nested (bool): Whether to return paths that are not direct children of the root directory. If True, all
                paths in the store will be yielded. Otherwise, only the top-level paths (i.e. direct children of the
                root path) will be yielded. This option is passed to FilePathGenerator initializer. Defaults to True.
            **kwargs: Other keyword arguments passed to the FilePathGenerator initializer. If a key is present in
                both `kwargs` and `self.storage_options`, the value from `kwargs` will be used.

        Yields:
            (str) Paths to files and directories in the store relative to the root directory.
        """
        yield from super().keys(regex_filter=".parquet", **kwargs)
