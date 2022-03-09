import typing as t

import numpy as np

from squirrel.driver import JsonlDriver
from squirrel.store import SquirrelStore


def test_keys(dummy_jsonl_store: SquirrelStore, num_samples: int) -> None:
    """Test keys method of the store"""
    keys = list(dummy_jsonl_store.keys())
    assert len(keys) == 2


def test_get_iter(dummy_jsonl_store: SquirrelStore, num_samples: int) -> None:
    """Test MessagePackDataLoader.get_iter"""
    it = JsonlDriver(url=dummy_jsonl_store.url).get_iter().collect()
    assert isinstance(it, list)
    assert len(list(it)) == num_samples


def test_custom_deser_hook(dummy_jsonl_store: SquirrelStore, num_samples: int) -> None:
    """Test custom Deser hook for JSONL decoding"""

    def _hook(dct: t.Dict) -> t.Any:
        if "meta" in dct:
            if "size" in dct["meta"] and "dtype" in dct["meta"]:
                return np.array(dct["image"], dtype=np.dtype(dct["meta"]["dtype"])).reshape(dct["meta"]["size"])
        return dct

    it = JsonlDriver(url=dummy_jsonl_store.url, deser_hook=_hook).get_iter().collect()
    assert isinstance(it, list)
    assert len([sample for sample in it]) == num_samples
