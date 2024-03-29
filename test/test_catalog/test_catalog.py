import fsspec
import pytest
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.cached import SimpleCacheFileSystem
from squirrel.catalog import Catalog, Source
from squirrel.catalog.catalog import CatalogKey, DummyCatalogSource
from squirrel.constants import URL
from squirrel.driver import Driver
from squirrel.framework.plugins.plugin_manager import register_driver, register_source
from squirrel.fsspec.fs import get_fs_from_url


def test_catalog_createempty() -> None:
    """Test creating a Catalog"""
    _ = Catalog()


def test_catalog_createsimple() -> None:
    """Test creating a Catalog"""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"url": "./test.csv"})

    assert "s" in cat
    assert "s1" not in cat


def test_catalog_versioning() -> None:
    """Test versioning a source in a Catalog using all allowed identifier types."""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"url": "./test1.csv"})
    cat["s"][cat["s"].version + 1] = Source("csv", driver_kwargs={"url": "./test2.csv"})
    cat["s", cat["s"].version + 1] = Source("csv", driver_kwargs={"url": "./test3.csv"})
    key = CatalogKey("s", cat["s"].version + 1)
    cat[key] = Source("csv", driver_kwargs={"url": "./test4.csv"})
    bad_id = "non-existing"

    # check contains
    assert "s" in cat
    for ver in range(1, 5):
        assert ("s", ver) in cat
        assert CatalogKey("s", ver) in cat
    assert bad_id not in cat
    assert (bad_id, 1) not in cat
    assert CatalogKey(bad_id, 1) not in cat

    # try to get non-existing source
    ret = cat[bad_id]
    assert isinstance(ret, DummyCatalogSource)
    with pytest.raises(KeyError):
        cat[bad_id, 1]

    # check getitem
    assert len(cat["s"].versions) == 4
    assert cat["s"][-1] == cat["s"]
    assert cat["s", -1] == cat["s"]
    assert cat[CatalogKey("s", -1)] == cat["s"]
    for ver in range(1, 5):
        assert cat["s", ver].driver_kwargs["url"] == f"./test{ver}.csv"

    # check entries distinct
    assert cat["s"][1] != cat["s"][2]
    assert cat["s", 1] != cat["s", 2]
    assert cat[CatalogKey("s", 1)] != cat[CatalogKey("s", 2)]

    # check delete
    del cat["s"][1]
    assert len(cat["s"].versions) == 3
    del cat["s", 2]
    assert len(cat["s"].versions) == 2
    del cat[key]
    assert len(cat["s"].versions) == 1

    del cat["s"]
    assert "s" not in cat


def test_wrong_identifier_type() -> None:
    """Test against common wrong identifier types."""
    cat = Catalog()
    iden, ver = "s", 2
    cat[iden, ver] = Source("csv", driver_kwargs={"url": "./test1.csv"})
    with pytest.raises(TypeError):
        cat[1337]
    with pytest.raises(KeyError):
        cat[1337, ver]
    with pytest.raises(KeyError):
        cat[1337, -1]
    with pytest.raises(KeyError):
        cat[1337, "-1"]
    with pytest.raises(KeyError):
        cat[iden, "-1"]


def test_catalog_copy() -> None:
    """Test deep copying  a Catalog"""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"url": "./test.csv"})
    cat["s"] = Source("csv", driver_kwargs={"url": "./test.csv"})

    assert cat == cat
    assert cat.copy() == cat
    assert cat.copy()["s"] == cat["s"]


def test_catalog_setapi() -> None:
    """Test merging of Catalogs"""
    cat1 = Catalog()
    cat1["s0"] = Source("csv", driver_kwargs={"url": "./test0.csv"})
    cat1["s1"] = Source("csv", driver_kwargs={"url": "./test1.csv"})
    cat1["s2"] = Source("csv", driver_kwargs={"url": "./test2.csv"})

    cat2 = Catalog()
    cat2["s0"] = Source("csv", driver_kwargs={"url": "./test0.csv"})
    cat2["s1"][1] = Source("csv", driver_kwargs={"url": "./test1.csv"})
    cat2["s1"][2] = Source("csv", driver_kwargs={"url": "./test11.csv"})
    cat2["s3"] = Source("csv", driver_kwargs={"url": "./test3.csv"})

    intersection = cat1.intersection(cat2)
    assert "s0" in intersection
    assert "s1" in intersection
    assert len(intersection) == 2
    assert len(intersection["s1"].versions) == 1

    union = cat1.union(cat2)
    assert "s0" in union
    assert "s1" in union
    assert "s2" in union
    assert "s3" in union
    assert len(union) == 4
    assert len(union["s1"].versions) == 2

    difference = cat1.difference(cat2)
    assert "s2" in difference
    assert "s3" in difference
    assert "s1" in difference
    assert len(difference["s1"].versions) == 1
    assert len(difference) == 3

    slice = cat2.slice(["s1"])
    assert len(slice) == 1
    assert len(slice["s1"].versions) == 2
    slice = cat1.slice(["s1"])
    assert len(slice) == 1
    assert len(slice["s1"].versions) == 1

    assert intersection.join(difference) == union

    cat3 = cat1.filter(lambda x: x.version == 1)
    assert len(cat3) == 3
    assert all(s.version == 1 for _, s in cat3.items())


def test_catalog_saveandload(test_path: URL) -> None:
    """Test Saving a catalog"""
    fp = f"{test_path}/example.yaml"

    cat1 = Catalog()
    cat1["s"] = Source("csv", driver_kwargs={"url": "./test.csv"})

    cat1.to_file(fp)
    cat2 = Catalog.from_files([fp])

    assert cat1 == cat2


def test_catalog_loadyaml(tmp_yaml_1: URL) -> None:
    """Test load from a serialized a catalog"""
    cat = Catalog.from_files([tmp_yaml_1])
    df = cat["cs"].get_driver().get_df()

    assert df["a"].iloc[0] == 1


def test_catalog_repr() -> None:
    """Test if repr return sorted keys"""
    cat = Catalog()
    cat["a"] = Source("dummy")
    cat["b"] = Source("dummy")
    cat["ab"] = Source("dummy")
    cat["c"] = Source("dummy")
    assert cat.__repr__() == "['a', 'ab', 'b', 'c']"


def test_get_fs_from_url() -> None:
    """Tests argument combinations given to the get_fs_from_url."""
    # test without storage_options
    fs = get_fs_from_url("gs://some-bucket/test.csv")
    assert isinstance(fs, AsyncFileSystem)
    assert fs.protocol == ("gcs", "gs")

    # correct way of using storage_options
    storage_options = {"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}
    fs = get_fs_from_url("gs://some-bucket/test.csv", **storage_options)
    assert isinstance(fs, SimpleCacheFileSystem)
    # fsspec automatically detects the protocol via target_protocol
    assert fs.protocol == ("gcs", "gs")
    # fsspec deletes the "protocol" key from the storage_options
    assert fs.storage_options == {"target_protocol": "gs", "cache_storage": "path/to/cache"}

    # incorrect way of using storage_options (note: users have to provide the target_protocol
    # even if they provide a url that starts with the protocol)
    storage_options = {"protocol": "simplecache", "cache_storage": "path/to/cache"}
    with pytest.raises(Exception) as exc_info:
        fs = get_fs_from_url("gs://some-bucket/test.csv", **storage_options)
    assert exc_info.errisinstance(ValueError)


def test_get_driver_storage_options() -> None:
    """
    Tests whether storage_options updating mechanism works. The mechanism enables users to update
    the existing storage_options of a CatalogSource via the get_driver method.
    """

    # some dummy storage_options
    so_cache = {
        "protocol": "simplecache",
        "target_protocol": "gs",
        "cache_storage": "path/to/cache",
    }
    so_rp = {"requester_pays": True}

    # create source and register in catalog
    cat = Catalog()
    cat["source"] = Source("csv", driver_kwargs={"url": "gs://some-bucket/test.csv", "storage_options": so_rp})

    # use default storage_options
    driver = cat["source"].get_driver()
    assert driver.storage_options == {"requester_pays": True}

    # update storage_options via get_driver
    driver = cat["source"].get_driver(storage_options=so_cache)
    assert driver.storage_options == {
        "protocol": "simplecache",
        "target_protocol": "gs",
        "cache_storage": "path/to/cache",
        "requester_pays": True,
    }

    # check if updating of existing storage_options works
    assert driver.storage_options["requester_pays"] == True  # noqa: E712
    driver = cat["source"].get_driver(storage_options={"requester_pays": False})
    assert driver.storage_options["requester_pays"] == False  # noqa: E712

    # update some kwarg of the driver that is not storage options
    driver = cat["source"].get_driver(url="gs://some-bucket/test2.csv")
    assert driver.url == "gs://some-bucket/test2.csv"


@pytest.fixture
def tmp_yaml_1(test_path: URL) -> URL:
    """Create a tmp yaml file under the path `f_path`. No need to teardown, cuz pytest will tear the entire tmp_path
    for you.
    """
    csv_path = f"{test_path}/test.csv"
    f_path = f"{test_path}/example.yaml"
    content = f"""
    !YamlCatalog
    version: 0.4.0
    sources:
    - !YamlSource
      identifier: cs
      driver_name: csv
      driver_kwargs:
        url: {csv_path}
    """
    with fsspec.open(f_path, "w") as f:
        f.write(content)

    # write source dummies for test catalog
    content = "a,b,c\n1,2,3"
    with fsspec.open(csv_path, "w") as f:
        f.write(content)

    return f_path


def test_catalog_plugin_driver() -> None:
    """Test custom driver"""

    class TestDriver(Driver):
        name = "testdriver"

        def __init__(self, welcome: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self.welcome = welcome

        def get_store(self, **kwargs) -> None:
            """Here to obey the API."""
            pass

        def say_hi(self, **kwargs) -> str:
            return f"Hello {self.welcome}!"

    register_driver(TestDriver)

    cat = Catalog()
    cat["test"] = Source("testdriver", driver_kwargs={"welcome": "Squirrel"})

    assert cat["test"].get_driver().say_hi() == "Hello Squirrel!"


def test_catalog_plugin_source() -> None:
    """Test plugin source"""
    s = Source("csv", driver_kwargs={"url": "./test1.csv"}, metadata={"hello": "world"})
    register_source("mysource", s)

    cat = Catalog.from_plugins()
    assert cat["mysource"].metadata["hello"] == "world"
