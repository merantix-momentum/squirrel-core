from __future__ import annotations

from typing import Literal, List, Dict

from deltalake import write_deltalake
from pyarrow.types import is_binary
import pyarrow as pa

from squirrel.driver.deltalake import serialize_np_to_msgpack
from squirrel.iterstream import Composable, IterableSource


def shard_to_record_batch(rows: List[Dict]):
    _keys = list(rows[0].keys())
    return pa.RecordBatch.from_pydict(
        {_keys[key_idx]: [rows[item_idx][_keys[key_idx]] for item_idx in range(len(rows))] for key_idx in range(len(_keys))}
    )


class PersistToDeltalake(Composable):
    def __init__(
            self,
            uri: str,
            shard_size: int,
            schema,
            mode: Literal["error", "append", "overwrite", "ignore"] = 'append'
    ):
        """
        Args:
            uri: the location that the data will be saved
            shard_size (int): number of items stored in one shard
        """
        super().__init__()
        self.uri = uri
        self.shard_size = shard_size
        self.schema = schema
        self.mode = mode
    def __iter__(self):
        it = IterableSource(self.source)
        if any([is_binary(i) for i in self.schema.types]):
            it = it.map(lambda x: serialize_np_to_msgpack(x))
        it = it.batched(self.shard_size, drop_last_if_not_full=False).map(shard_to_record_batch).map(
            lambda rec_batch: write_deltalake(self.uri, rec_batch, schema=self.schema, mode=self.mode)
        )
        yield from it
