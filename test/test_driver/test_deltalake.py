import datetime
import tempfile
from functools import partial
from typing import Any, List, Dict, Tuple

import numpy as np
import pyarrow as pa
from deltalake import DeltaTable
import pytest

from squirrel.driver.deltalake import DeltalakeDriver
from squirrel.store.deltalake import PersistToDeltalake
from squirrel.iterstream import IterableSource, FilePathGenerator


def test_basic_writing_and_reading():
    samples, schema = temporal_dataset(1, 10, 2)
    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=3, schema=schema, mode="append")
        ).join()
        assert len(FilePathGenerator(tmp_dir, nested=True).collect()) == 8
        retrieved = DeltalakeDriver(url=tmp_dir).get_iter().collect()
        assert retrieved == samples


def test_numpy(np_dataset: Tuple[List[Dict[str, Any]], pa.Schema]):
    """Test writing and reading dictionaries with numpy array"""
    samples, schema = np_dataset
    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=3, schema=schema, mode="append")
        ).join()
        assert len(FilePathGenerator(tmp_dir, nested=True).collect()) == 8
        retrieved = DeltalakeDriver(url=tmp_dir).get_iter().collect()
        assert isinstance(retrieved[0]["img"], np.ndarray) and isinstance(samples[0]["img"], np.ndarray)
        assert sum(i["img"].sum() for i in retrieved) == sum(i["img"].sum() for i in samples)


def test_version() -> None:
    batch_size = 1000  # big number to avoid creating more than one version with a single PersistToDeltalake
    samples, schema = temporal_dataset(1, 10, 2)
    updated, _ = temporal_dataset(8, 5, 10)
    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=batch_size, schema=schema, mode="append")
        ).join()

        IterableSource(updated).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=batch_size, schema=schema, mode="overwrite")
        ).join()

        dlake = DeltaTable(tmp_dir)
        assert dlake.version() == 1
        retrieved = DeltaTable(tmp_dir, version=0).to_pandas().to_dict(orient="record")
        assert retrieved == samples

        retrieved = DeltaTable(tmp_dir, version=1).to_pandas().to_dict(orient="record")
        assert retrieved == updated


def test_timetravel() -> None:
    samples1, schema = temporal_dataset(1, 10, 2)
    samples2, _ = temporal_dataset(8, 10, 10)

    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples1).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=30, schema=schema, mode="append")
        ).join()

        t = datetime.datetime.now(datetime.timezone.utc).isoformat()

        IterableSource(samples2).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=30, schema=schema, mode="append")
        ).join()

        dlake = DeltaTable(tmp_dir)
        dlake.load_with_datetime(datetime_string=t)
        assert dlake.to_pandas().to_dict(orient="record") == samples1


def test_get_subset_of_columns(np_dataset) -> None:
    samples, schema = np_dataset
    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=20, schema=schema, mode="append")
        ).join()
        retrieved = DeltalakeDriver(url=tmp_dir).get_iter().collect()
        assert retrieved == samples
        retrieved_labels = DeltalakeDriver(tmp_dir).get_iter(keys=["lable"]).collect()
        assert len(retrieved_labels[0].keys()) == 1
        assert [i["lable"] for i in retrieved_labels] == [i["lable"] for i in samples]


def temporal_dataset(start, size, factor) -> Tuple[List[Dict[str, Any]], pa.Schema]:
    samples = [{"timestamp": i, "value": i * factor} for i in range(start, start + size)]
    schema = pa.schema(
        [
            ("timestamp", pa.int64()),
            ("value", pa.int64()),
        ]
    )
    return samples, schema


@pytest.fixture(scope="module")
def np_dataset() -> Tuple[List[Dict[str, Any]], pa.Schema]:
    samples = [{"img": np.random.random((1, 1)), "lable": int(np.random.randint(0, 5))} for _ in range(10)]
    schema = pa.schema(
        [
            ("img", pa.binary()),
            ("lable", pa.int64()),
        ]
    )
    return samples, schema
