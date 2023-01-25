from __future__ import annotations

from typing import Any, Dict

import numpy as np
from deltalake import DeltaTable

from squirrel.driver import IterDriver
from squirrel.iterstream import Composable, IterableSource
from squirrel.serialization.msgpack import MessagepackSerializer

__all__ = [
    "DeltalakeDriver",
]


def serialize_np_to_msgpack(x):
    for k in x.keys():
        if isinstance(x[k], np.ndarray):
            x[k] = MessagepackSerializer.serialize(x[k])
    return x


class RecordBatchToItem(Composable):

    def __iter__(self):
        for item in self.source:
            item = item.to_pydict()
            _keys = list(item.keys())
            for idx in range(len(item[_keys[0]])):
                yield {k: item[k][idx] for k in _keys}


class DeltalakeDriver(IterDriver):

    name = "deltalake"

    def __init__(self, url: str, storage_options: dict[str, Any] | None = None, **kwargs):
        if "store" in kwargs:
            raise ValueError("Store of MessagepackDriver is fixed, `store` cannot be provided.")
        super().__init__()
        self.url = url
        self.storage_options = storage_options

    def get_iter(
            self,
            flatten: bool = True,
            version: int | None = None,
            storage_options: Dict[str, Any] | None = None,
            without_files: bool = False,
            **kwargs
    ) -> Composable:
        return IterableSource(
            self.get_delta_table(version=version, without_files=without_files).to_pyarrow_dataset().to_batches()
        ).compose(RecordBatchToItem)

    def get_delta_table(self, version: int | None = None, without_files: bool = False):
        return DeltaTable(
            table_uri=self.url, version=version, storage_options=self.storage_options, without_files=without_files
        )
