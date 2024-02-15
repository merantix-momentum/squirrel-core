from collections import Counter
import tempfile
from typing import Dict, List

import ray
from test.conftest import get_records_with_np
import torch.utils.data as tud
import polars as pl
import pytest
import numpy as np


from squirrel.driver.msgpack import MessagepackDriver
from squirrel.store.parquet_store import ParquetStore
from squirrel.driver.streaming_parquet import StreamingParquetDriver
from squirrel.iterstream.source import IterableSource
# from squirrel.constants import URL


def assert_equal_arrays(arrays: List[np.array]) -> None:
    """Assert arrays are equal by summing each and comparing the result"""
    sums = np.array([np.sum(x) for x in arrays])
    assert np.isclose(sum(sums - sums[0]), 0)


def test_numpy_with_ray(np_ray):
    _path, _data = np_ray
    ds = ray.data.read_numpy(_path)
    ds = list(ds.iter_rows())
    assert all((ds[i]['data'] == _data[i]['image']).all() for i in range(len(_data)))


def test_parquet_with_ray(image_parquet_ray):
    _path, _data = image_parquet_ray
    ds = ray.data.read_parquet(_path)
    ds = list(ds.iter_rows())
    assert all((ds[i]['image'] == _data[i]['image']).all() for i in range(len(_data)))


def test_write_deltalake_read_ray(no_image_parquet_deltalake_with_ray):
    _path, _data = no_image_parquet_deltalake_with_ray
    ds = ray.data.read_parquet(_path)
    ds = list(ds.iter_rows())
    for k in ["id", "lable", "image"]:
        assert [ds[i][k] == _data[i][k] for i in range(len(_data))]


@pytest.mark.parametrize("directory", ['normal_parquet_ray', 'no_image_parquet_deltalake_with_ray'])
def test_streaming_prquet_driver(directory, request):
    _path, _data = request.getfixturevalue(directory)
    it = StreamingParquetDriver(url=_path).get_iter().collect()

    ps = ParquetStore(url=_path)

    _keys = list(ps.keys())
    assert all([True for k in _keys if "prquet" in k])
    retrieved = []
    for k in _keys:
        retrieved.extend(ps.get(k))

    assert sorted([i['lable'] for i in it]) == sorted([i['lable'] for i in _data]) == sorted([i['lable'] for i in retrieved])


def test_polars_streaming(normal_parquet_ray):
    _path, _data = normal_parquet_ray
    lazy_df = pl.scan_parquet(_path + "/*")
    it = IterableSource(lazy_df.collect(streaming=True).iter_rows(named=True)).collect()
    c1 = Counter([i['lable'] for i in _data])
    c2 = Counter([i['lable'] for i in it])
    assert c1 == c2


def test_polars_streaming_deltalake(no_image_parquet_deltalake_with_ray):
    _path, _data = no_image_parquet_deltalake_with_ray
    lazy_df = pl.scan_delta(_path)
    it = IterableSource(lazy_df.collect(streaming=True).iter_rows(named=True)).collect()
    c1 = Counter([i['lable'] for i in _data])
    c2 = Counter([i['lable'] for i in it])
    assert c1 == c2

@pytest.mark.parametrize("directory", ['normal_parquet_ray', 'no_image_parquet_deltalake_with_ray'])
def test_polars_streaming(directory, request):
    _path, _data = request.getfixturevalue(directory)

    if directory == "normal_parquet_ray":
        lazy_df = pl.scan_parquet(_path + "/*")
    else:
        lazy_df = pl.scan_delta(_path)

    it = IterableSource(lazy_df.collect(streaming=True).iter_rows(named=True)).collect()
    c1 = Counter([i['lable'] for i in _data])
    c2 = Counter([i['lable'] for i in it])
    assert c1 == c2


def test_streaming_parquet_with_pytorch_dataloader(normal_parquet_ray):
    _path, _data = normal_parquet_ray
    ds = ray.data.read_parquet(_path)
    num_workers = 4
    batch_size = 5

    it = StreamingParquetDriver(url=_path).get_iter().split_by_worker_pytorch().to_torch_iterable()
    dl = list(tud.DataLoader(it, num_workers=num_workers))
    dl = [i["lable"].cpu().numpy()[0] for i in dl]
    c1 = Counter(dl)
    c2 = Counter([i['lable'] for i in _data])
    dl2 = list(ds.iter_torch_batches(batch_size=batch_size))
    dl2_ = []
    for i in dl2:
        _np = i["lable"].cpu().numpy()
        dl2_.extend(list(_np))
    c3 = Counter(dl2_)
    assert c1 == c2 == c3


def test_msgpack_with_ray():
    """Test more complext scenario of distributed load (and potentially transformation) with Ray wrapped in IterableSource"""

    with tempfile.TemporaryDirectory() as tmp_dir:
        _data = get_records_with_np()
        st = MessagepackDriver(url=tmp_dir).store
        IterableSource(_data).batched(10, drop_last_if_not_full=False).map(st.set).collect()
        keys = list(st.keys())
        
        def _load_sample(k):
            k = k["item"]
            st = MessagepackDriver(url=tmp_dir).store
            data = IterableSource(st.get(k)).collect()
            return {"item": {"inner": data}}
        
        ds = (ray.data.from_items(keys)
            .map(_load_sample)
        )
        res = IterableSource(ds.iter_rows()).map(lambda x: x["item"]["inner"]).flatten().collect()

        assert_equal_arrays([[i["image"] for i in _data], [i["image"] for i in res]])