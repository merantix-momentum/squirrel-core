from collections import Counter
import tempfile
from typing import Dict, Iterable, List

import ray
from squirrel.catalog.catalog import Catalog
from squirrel.constants import URL
from squirrel.store.parquet_store import DeltalakeStore
from test.conftest import get_records_with_np, get_records_without_np
import torch.utils.data as tud
import polars as pl
import pytest
import numpy as np


from squirrel.store import ParquetStore
from squirrel.driver import MessagepackDriver, DeltalakeDriver, PolardParquetDriver, StreamingParquetDriver
from squirrel.iterstream.source import FilePathGenerator, IterableSource


def assert_equal_arrays(arrays: List[np.array]) -> None:
    """Assert arrays are equal"""
    _arrays = [np.array(sorted(list_of_arrays, key=lambda x: np.sum(x))) for list_of_arrays in arrays]
    return np.sum(_arrays - _arrays[0])


def test_deltalake_store(test_path: URL) -> None:
    """Test deltalake store"""
    _data = get_records_without_np(10)
    dst = DeltalakeStore(test_path)
    IterableSource(_data).batched(5).map(dst.set).join()
    _ret = DeltalakeDriver(test_path).get_iter().collect()
    assert len(_ret) == len(_data)
    all_paths = FilePathGenerator(test_path, nested=True).collect()
    assert len([p for p in all_paths if ".parquet" in p]) == 2
    assert len([p for p in all_paths if ".json" in p]) == len(list(dst.keys())) == 2


def test_parquet_iter_ray(normal_parquet_ray: Iterable) -> None:
    """Test StreamingParquetDriver.get_iter_ray"""
    _path, _data = normal_parquet_ray
    d = StreamingParquetDriver(_path)
    nrows = len(d.store)
    it = d.get_iter_ray().collect()
    assert len(it) == len(_data) == nrows
    assert_equal_arrays([[i["lable"] for i in it], [i["lable"] for i in _data]])


def test_polars_parquet_driver(normal_parquet_ray: Iterable) -> None:
    """Test reading parquet with PolardParquetDriver"""
    _path, _data = normal_parquet_ray
    d1 = PolardParquetDriver(_path).get_iter().collect()
    d2 = StreamingParquetDriver(_path).get_iter().collect()

    assert len(PolardParquetDriver(_path).store) == len(StreamingParquetDriver(_path).store)

    for i in range(1, 4):
        pdr = PolardParquetDriver(_path).query(f"SELECT lable FROM frame WHERE lable = {i}").collect()
        assert np.sum(pdr.to_numpy()) == len(pdr) * i

    assert_equal_arrays([[i["lable"] for i in d1], [i["lable"] for i in d2], [i["lable"] for i in _data]])


def test_deltalake_driver(no_image_parquet_deltalake_with_ray: Iterable) -> None:
    """Test deltalake"""
    _path, _data = no_image_parquet_deltalake_with_ray
    d = DeltalakeDriver(_path)
    dt = d.deltatable().to_pandas().to_dict(orient="record")
    assert dt == _data


def test_partitioned_parquet(partitioned_parquet: Iterable) -> None:
    """Test partitioned parquet with StreamingParquetDriver"""
    _path, _data = partitioned_parquet
    ds = ray.data.read_parquet(_path)
    ds = list(ds.iter_rows())
    d = StreamingParquetDriver(_path).get_iter().collect()
    assert_equal_arrays([[i["image"] for i in d], [i["image"] for i in _data]])
    assert_equal_arrays([[i["lable"] for i in ds], [i["lable"] for i in _data]])


def test_streaming_parquet_driver_in_catalog(parquet_catalog: Catalog) -> None:
    """Test StreamingParquetDriver registration win catalog"""
    d: StreamingParquetDriver = parquet_catalog["parquet"].get_driver()
    it = d.get_iter().collect()
    assert len(it) == parquet_catalog["parquet"].metadata["num_samples"]


def test_numpy_with_ray(np_ray: Iterable) -> None:
    """Test reading numpy directory with ray"""
    _path, _data = np_ray
    ds = ray.data.read_numpy(_path)
    ds = list(ds.iter_rows())
    assert len(ds) == len(_data)
    assert_equal_arrays([[[i["data"] for i in ds], [i["image"] for i in _data]]])


def test_reading_parquet_that_contains_array_with_ray(image_parquet_ray: Iterable) -> None:
    """Test reading parquet that contains arrays with ray"""
    _path, _data = image_parquet_ray
    ds = ray.data.read_parquet(_path)
    ds = list(ds.iter_rows())
    assert len(ds) == len(_data)
    assert_equal_arrays([[[i["image"] for i in ds], [i["image"] for i in _data]]])


def test_reading_deltalake_with_ray(no_image_parquet_deltalake_with_ray: Iterable) -> None:
    """Test reading deltalake with ray"""
    _path, _data = no_image_parquet_deltalake_with_ray
    ds = ray.data.read_parquet(_path)
    ds = list(ds.iter_rows())
    for k in ["id", "lable", "image"]:
        assert [ds[i][k] == _data[i][k] for i in range(len(_data))]


@pytest.mark.parametrize("directory", ["normal_parquet_ray", "no_image_parquet_deltalake_with_ray"])
def test_streaming_prquet_driver(directory: str, request: pytest.FixtureRequest) -> None:
    """Test streaing parquet"""
    _path, _data = request.getfixturevalue(directory)
    it = StreamingParquetDriver(url=_path).get_iter().collect()

    ps = ParquetStore(url=_path)

    _keys = list(ps.keys())
    assert all([True for k in _keys if "prquet" in k])
    retrieved = []
    for k in _keys:
        retrieved.extend(ps.get(k))

    assert sorted(i["lable"] for i in it) == sorted(i["lable"] for i in _data) == sorted(i["lable"] for i in retrieved)


@pytest.mark.parametrize("directory", ["normal_parquet_ray", "no_image_parquet_deltalake_with_ray"])
def test_polars_streaming(directory: str, request: pytest.FixtureRequest) -> None:
    """Test polars streaming"""
    _path, _data = request.getfixturevalue(directory)

    if directory == "normal_parquet_ray":
        lazy_df = pl.scan_parquet(_path + "/*")
    else:
        lazy_df = pl.scan_delta(_path)

    it = IterableSource(lazy_df.collect(streaming=True).iter_rows(named=True)).collect()
    c1 = Counter([i["lable"] for i in _data])
    c2 = Counter([i["lable"] for i in it])
    assert c1 == c2


def test_polars_streaming_deltalake(no_image_parquet_deltalake_with_ray: Iterable) -> None:
    """Test polars streaming with deltalake"""
    _path, _data = no_image_parquet_deltalake_with_ray
    lazy_df = pl.scan_delta(_path)
    it = IterableSource(lazy_df.collect(streaming=True).iter_rows(named=True)).collect()
    c1 = Counter([i["lable"] for i in _data])
    c2 = Counter([i["lable"] for i in it])
    assert c1 == c2


def test_streaming_parquet_with_pytorch_dataloader(normal_parquet_ray: Iterable) -> None:
    """Test streaming parquet with pytorch dataloader"""
    _path, _data = normal_parquet_ray
    ds = ray.data.read_parquet(_path)
    num_workers = 2
    batch_size = 5

    it = StreamingParquetDriver(url=_path).get_iter().split_by_worker_pytorch().to_torch_iterable()
    dl = list(tud.DataLoader(it, num_workers=num_workers))
    dl = [i["lable"].cpu().numpy()[0] for i in dl]
    c1 = Counter(dl)
    c2 = Counter([i["lable"] for i in _data])
    dl2 = list(ds.iter_torch_batches(batch_size=batch_size))
    dl2_ = []
    for i in dl2:
        _np = i["lable"].cpu().numpy()
        dl2_.extend(list(_np))
    c3 = Counter(dl2_)
    assert c1 == c2 == c3


def test_msgpack_with_ray() -> None:
    """
    Test more complext scenario of distributed load (and potentially transformation) with
    Ray wrapped in IterableSource
    """

    with tempfile.TemporaryDirectory() as tmp_dir:
        _data = get_records_with_np()
        st = MessagepackDriver(url=tmp_dir).store
        IterableSource(_data).batched(10, drop_last_if_not_full=False).map(st.set).collect()
        keys = list(st.keys())

        def _load_sample(k: Dict) -> Dict:
            k = k["item"]
            st = MessagepackDriver(url=tmp_dir).store
            data = IterableSource(st.get(k)).collect()
            return {"item": {"inner": data}}

        ds = ray.data.from_items(keys).map(_load_sample)
        res = IterableSource(ds.iter_rows()).map(lambda x: x["item"]["inner"]).flatten().collect()

        assert_equal_arrays([[i["image"] for i in _data], [i["image"] for i in res]])
