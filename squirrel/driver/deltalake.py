from __future__ import annotations

from typing import Any

import numpy as np
from deltalake import DeltaTable

from squirrel.driver import IterDriver
from squirrel.iterstream import Composable, IterableSource
from squirrel.serialization.msgpack import MessagepackSerializer

__all__ = [
    "DeltalakeDriver",
]


def serialize_ndarray_to_msgpack(x: dict):
    """Serializes any value in the dictionary to messagepack if it is a numpy array"""
    res = {}
    for k in x.keys():
        if isinstance(x[k], np.ndarray):
            res[k] = MessagepackSerializer.serialize(x[k])
        else:
            res[k] = x[k]
    return res


def deserialize_messagepack_to_ndarray(x):
    """De-serializes any value in the dictionary from messagepack if it is of type `bytes`"""
    res = {}
    for k in x.keys():
        if isinstance(x[k], bytes):
            res[k] = MessagepackSerializer.deserialize(x[k])
        else:
            res[k] = x[k]
    return res


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
        storage_options: dict[str, Any] | None = None,
        without_files: bool = False,
        keys: list[str] | None = None,
        datetime_string=None,
        **kwargs,
    ) -> Composable:
        _iter = self.get_delta_table(version=version, without_files=without_files)
        if datetime_string is not None:
            _iter.load_with_datetime(datetime_string=datetime_string)
        if keys is not None:
            _iter = _iter.to_pyarrow_table(columns=keys).to_batches()
        else:
            _iter = _iter.to_pyarrow_dataset().to_batches()
        return IterableSource(_iter).compose(RecordBatchToItem).map(deserialize_messagepack_to_ndarray)

    def get_delta_table(self, version: int | None = None, without_files: bool = False):
        return DeltaTable(
            table_uri=self.url, version=version, storage_options=self.storage_options, without_files=without_files
        )
