from squirrel.catalog import Catalog, CatalogKey
from squirrel.catalog.source import Source
from squirrel.constants import URL


def test_combiner_in_catalog(test_path: URL) -> None:
    """Test if sources can be combined into a source combiner"""
    c = Catalog()

    c["train"] = Source("file", driver_kwargs={"path": test_path + "train.dummy"})
    c["val"] = Source("file", driver_kwargs={"path": test_path + "val.dummy"})
    c["test"] = Source("file", driver_kwargs={"path": test_path + "test.dummy"})

    c["combined"] = Source(
        "source_combiner",
        driver_kwargs={
            "subsets": {
                "subset1": CatalogKey("train", 1),
                "subset2": CatalogKey("val", 1),
                "subset3": CatalogKey("test", 1),
            }
        },
    )

    d = c["combined"].get_driver()
    assert len(d.subsets) == 3
    assert d.get_source("subset1").get_driver().path == test_path + "train.dummy"
    assert d.get_source("subset2").get_driver().path == test_path + "val.dummy"
    assert d.get_source("subset3").get_driver().path == test_path + "test.dummy"


def test_copy_combiner(test_path: URL) -> None:
    """Test if source combiner can be serialized"""
    c = Catalog()
    c["train"] = Source("file", driver_kwargs={"path": test_path + "train.dummy"})
    c["combined"] = Source(
        "source_combiner",
        driver_kwargs={
            "subsets": {
                "subset1": CatalogKey("train", 1),
            }
        },
    )

    assert c.copy() == c
