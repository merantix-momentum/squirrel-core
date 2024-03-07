import ray
import pytest

from squirrel.catalog import Catalog
from squirrel.driver.directory import DirectoryDriver
from squirrel.serialization import PNGSerializer
from squirrel.store import DirectoryStore
from squirrel.iterstream.source import FilePathGenerator, IterableSource
from test.test_driver.test_parquet_driver import assert_equal_arrays


@pytest.mark.parametrize("directory", ["png_image_directory"])
def test_directory_driver_with_images(directory: str, request: pytest.FixtureRequest) -> None:
    """Test directory driver with images"""
    _path, _data = request.getfixturevalue(directory)
    ds = ray.data.read_images(_path)
    dd = DirectoryDriver(_path, file_format="png")
    a = dd.get_iter().collect()
    b = IterableSource(dd.ray.iter_rows()).map(lambda x: x["image"]).collect()
    c = IterableSource(ds.iter_rows()).map(lambda x: x["image"]).collect()

    dstore = DirectoryStore(_path, serializer=PNGSerializer())
    d = [dstore.get(_k) for _k in dstore.keys()]

    assert_equal_arrays([a, b, c, d, _data])


# TODO: failing
# @pytest.mark.parametrize("directory", ["numpy_directory", "np_ray"])
# def test_directory_with_numpy(directory: str, request: pytest.FixtureRequest) -> None:
#     """Test directory driver with numpy"""
#     _path, _data = request.getfixturevalue(directory)

#     if type(_data[0]) != np.ndarray:
#         _data = [i["image"] for i in _data]
#     ds = ray.data.read_numpy(_path)
#     dd = DirectoryDriver(_path, file_format="npy")
#     a = dd.get_iter().collect()
#     b = IterableSource(dd.ray.iter_rows()).map(lambda x: x["data"]).collect()
#     c = IterableSource(ds.iter_rows()).map(lambda x: x["data"]).collect()
#     dstore = DirectoryStore(_path, serializer=NumpySerializer())
#     d = [dstore.get(_k) for _k in dstore.keys()]
#     assert_equal_arrays([a, b, c, d, _data])


def test_ray_iter(directory_img_catalog: Catalog) -> None:
    """Test DirectoryDriver.get_iter_ray() for images"""
    d = directory_img_catalog["im"].get_driver()
    it = d.get_iter().collect()
    it_ray = d.get_iter_ray().collect()
    assert len(it) == len(it_ray)
    assert_equal_arrays([it, it_ray])


def test_directory_store(directory_np_catalog: Catalog) -> None:
    """Test directory store"""
    d: DirectoryDriver = directory_np_catalog["np"].get_driver()
    _it = d.get_iter().collect()
    _paths = FilePathGenerator(directory_np_catalog["np"].driver_kwargs["url"]).collect()
    _keys = list(d.keys())
    assert len(_it) == len(_keys) == len(_paths)
