from __future__ import annotations

from typing import Literal, Mapping

from deltalake import write_deltalake
from deltalake import DeltaTable
from pyarrow._dataset_parquet import ParquetFileWriteOptions
import pyarrow.fs as pa_fs
from pyarrow.types import is_binary
import pyarrow as pa
from fsspec.spec import AbstractFileSystem

from squirrel.driver.deltalake import serialize_ndarray_to_msgpack
from squirrel.fsspec.fs import get_fs_from_url
from squirrel.iterstream import Composable, IterableSource


def shard_to_record_batch(rows: list[dict]):
    _keys = list(rows[0].keys())
    return pa.RecordBatch.from_pydict(
        {
            _keys[key_idx]: [rows[item_idx][_keys[key_idx]] for item_idx in range(len(rows))]
            for key_idx in range(len(_keys))
        }
    )


class PersistToDeltalake(Composable):
    def __init__(
        self,
        uri: str,
        *,
        shard_size: int,
        schema: pa.Schema | None = None,
        partition_by: list[str] | None = None,
        mode: Literal["error", "append", "overwrite", "ignore"] = "append",
        filesystem: pa_fs.FileSystem | AbstractFileSystem | None = None,
        file_options: ParquetFileWriteOptions | None = None,
        max_open_files: int = 1024,
        max_rows_per_file: int = 10 * 1024 * 1024,
        min_rows_per_group: int = 64 * 1024,
        max_rows_per_group: int = 128 * 1024,
        name: str | None = None,
        description: str | None = None,
        configuration: Mapping[str, str] | None = None,
        overwrite_schema: bool = False,
        storage_options: dict[str, str] | None = None,
    ):
        """
        Args:
            uri: the location that the data will be saved
            shard_size (int): number of items stored in one shard
        """
        super().__init__()
        self.uri = uri
        self.shard_size = shard_size
        self.mode = mode
        self.partition_by = partition_by
        self.fs = filesystem
        self.mode = mode
        self.file_options = file_options
        self.max_open_files = max_open_files
        self.max_rows_per_file = max_rows_per_file
        self.min_rows_per_group = min_rows_per_group
        self.max_rows_per_group = max_rows_per_group
        self.name = name
        self.description = description
        self.configuration = configuration
        self.overwrite_schema = overwrite_schema
        self.storage_options = storage_options if storage_options is not None else {}
        if not (isinstance(self.fs, AbstractFileSystem) or isinstance(self.fs, pa_fs.FileSystem)):
            self.fs = (get_fs_from_url(self.uri, **self.storage_options),)

        if schema == None:
            self.schema = self._get_schema_from_existing_store()
        else:
            self.schema = schema

    def _get_schema_from_existing_store(self):
        return DeltaTable(self.uri).schema().to_pyarrow()

    def __iter__(self):
        it = IterableSource(self.source)
        if any([is_binary(i) for i in self.schema.types]):
            it = it.map(lambda x: serialize_ndarray_to_msgpack(x))
        it = (
            it.batched(self.shard_size, drop_last_if_not_full=False)
            .map(shard_to_record_batch)
            .map(
                lambda rec_batch: write_deltalake(
                    self.uri,
                    rec_batch,
                    schema=self.schema,
                    mode=self.mode,
                    partition_by=self.partition_by,
                    file_options=self.file_options,
                    max_open_files=self.max_open_files,
                    max_rows_per_file=self.max_rows_per_file,
                    min_rows_per_group=self.min_rows_per_group,
                    max_rows_per_group=self.max_rows_per_group,
                    name=self.name,
                    description=self.description,
                    configuration=self.configuration,
                    overwrite_schema=self.overwrite_schema,
                    storage_options=self.storage_options,
                )
            )
        )
        yield from it
