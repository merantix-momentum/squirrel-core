import fsspec
import pytest

from squirrel.catalog import Catalog, Source
from squirrel.constants import URL
from squirrel.driver import Driver
from squirrel.framework.plugins.plugin_manager import register_driver, register_source


def test_catalog_createempty() -> None:
    """Test creating a Catalog"""
    _ = Catalog()


def test_catalog_createsimple() -> None:
    """Test creating a Catalog"""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"path": "./test.csv"})

    assert "s" in cat
    assert "s1" not in cat


def test_catalog_versioning() -> None:
    """Test versioning source in a Catalog"""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"path": "./test1.csv"})
    cat["s"][cat["s"].version + 1] = Source("csv", driver_kwargs={"path": "./test2.csv"})

    assert cat["s"][-1] == cat["s"]
    assert len(cat["s"].versions) == 2
    assert cat["s"][1] != cat["s"][2]

    del cat["s"][1]
    assert len(cat["s"].versions) == 1

    del cat["s"]
    assert "s" not in cat


def test_catalog_copy() -> None:
    """Test deep copying  a Catalog"""
    cat = Catalog()
    cat["s"] = Source("csv", driver_kwargs={"path": "./test.csv"})
    cat["s"] = Source("csv", driver_kwargs={"path": "./test.csv"})

    assert cat == cat
    assert cat.copy() == cat
    assert cat.copy()["s"] == cat["s"]


def test_catalog_setapi() -> None:
    """Test merging of Catalogs"""
    cat1 = Catalog()
    cat1["s0"] = Source("csv", driver_kwargs={"path": "./test0.csv"})
    cat1["s1"] = Source("csv", driver_kwargs={"path": "./test1.csv"})
    cat1["s2"] = Source("csv", driver_kwargs={"path": "./test2.csv"})

    cat2 = Catalog()
    cat2["s0"] = Source("csv", driver_kwargs={"path": "./test0.csv"})
    cat2["s1"][1] = Source("csv", driver_kwargs={"path": "./test1.csv"})
    cat2["s1"][2] = Source("csv", driver_kwargs={"path": "./test11.csv"})
    cat2["s3"] = Source("csv", driver_kwargs={"path": "./test3.csv"})

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
    cat1["s"] = Source("csv", driver_kwargs={"path": "./test.csv"})

    cat1.to_file(fp)
    cat2 = Catalog.from_files([fp])

    assert cat1 == cat2


def test_catalog_loadyaml(tmp_yaml_1: URL) -> None:
    """Test load from a serialized a catalog"""
    cat = Catalog.from_files([tmp_yaml_1])
    df = cat["cs"].get_driver().get_df()

    assert df["a"].compute().iloc[0] == 1


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
        path: {csv_path}
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
            super().__init__(*kwargs)
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
    s = Source("csv", driver_kwargs={"path": "./test1.csv"}, metadata={"hello": "world"})
    register_source("mysource", s)

    cat = Catalog.from_plugins()
    assert cat["mysource"].metadata["hello"] == "world"
