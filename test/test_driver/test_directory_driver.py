import ray
import pytest
import numpy as np


from squirrel.driver.directory import DirectoryDriver
from squirrel.serialization.png import PNGSerializer
from squirrel.serialization.np import NumpySerializer
from squirrel.store.directory_store import DirectoryStore
from squirrel.iterstream.source import IterableSource
from test.test_driver.test_parquet_driver import assert_equal_arrays


@pytest.mark.parametrize("directory", ["png_image_directory"])
def test_directory_driver_with_images(directory, request):
    _path, _data = request.getfixturevalue(directory)
    ds = ray.data.read_images(_path)
    dd = DirectoryDriver(_path, file_format="png")
    a = dd.get_iter().collect()
    b = IterableSource(dd.ray.iter_rows()).map(lambda x: x["image"]).collect()
    c = IterableSource(ds.iter_rows()).map(lambda x: x["image"]).collect()

    dstore = DirectoryStore(_path, serializer=PNGSerializer)
    d = [dstore.get(_k) for _k in dstore.keys()]

    assert_equal_arrays([a, b, c, d, _data])


@pytest.mark.parametrize("directory", ["numpy_directory", "np_ray"])
def test_directory_with_numpy(directory, request):
    _path, _data = request.getfixturevalue(directory)

    if type(_data[0]) != np.ndarray:
        _data = [i["image"] for i in _data]
    ds = ray.data.read_numpy(_path)
    dd = DirectoryDriver(_path, file_format="npy")
    a = dd.get_iter().collect()
    b = IterableSource(dd.ray.iter_rows()).map(lambda x: x["data"]).collect()
    c = IterableSource(ds.iter_rows()).map(lambda x: x["data"]).collect()
    dstore = DirectoryStore(_path, serializer=NumpySerializer)
    d = [dstore.get(_k) for _k in dstore.keys()]
    assert_equal_arrays([a, b, c, d, _data])
