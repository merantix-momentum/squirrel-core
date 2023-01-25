import tempfile
from functools import partial

import numpy as np
import pyarrow as pa


from squirrel.driver.deltalake import DeltalakeDriver
from squirrel.store.deltalake import PersistToDeltalake
from squirrel.iterstream import IterableSource, FilePathGenerator


def test_basic_writing_and_reading():
    samples = [{"item": i, "double": i*2} for i in range(10)]
    schema = pa.schema([
        ('item', pa.int64()),
        ('double', pa.int64()),
    ])
    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=3, schema=schema, mode='append')
        ).join()
        assert len(FilePathGenerator(tmp_dir, nested=True).collect()) == 8
        retrieved = DeltalakeDriver(url=tmp_dir).get_iter().collect()
        assert retrieved == samples


def test_numpy():
    """Test writing and reading dictionaries with numpy array"""
    samples = [{"img": np.random.random((5, 5, 1)), "lable": int(np.random.randint(0, 5))} for _ in range(10)]
    schema = pa.schema([
        ('img', pa.binary()),
        ('lable', pa.int64()),
    ])

    with tempfile.TemporaryDirectory() as tmp_dir:
        IterableSource(samples).compose(
            partial(PersistToDeltalake, uri=tmp_dir, shard_size=3, schema=schema, mode='append')
        ).join()
        assert len(FilePathGenerator(tmp_dir, nested=True).collect()) == 8
        retrieved = DeltalakeDriver(url=tmp_dir).get_iter().collect()
        assert retrieved == samples
