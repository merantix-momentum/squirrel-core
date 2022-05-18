from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from squirrel.catalog import Catalog, CatalogKey
from squirrel.catalog.source import Source
from squirrel.constants import URL


def test_combiner_in_catalog() -> None:
    """Test if sources can be combined into a source combiner"""
    c = Catalog()
    tmpdir = TemporaryDirectory()

    for split in ("train", "val", "test"):
        fname = f"{tmpdir.name}/{split}.csv"
        data = pd.DataFrame(dict(split=[split]))  # one row, one col df, only value of "split" changes
        data.to_csv(fname)
        c[split] = Source("csv", driver_kwargs={"path": fname})

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
    assert d.get_source("subset1").get_driver().path == f"{tmpdir.name}/train.csv"
    assert d.get_source("subset2").get_driver().path == f"{tmpdir.name}/val.csv"
    assert d.get_source("subset3").get_driver().path == f"{tmpdir.name}/test.csv"
    all_rows = d.get_iter().collect()
    assert [r.split for r in all_rows] == ["train", "val", "test"]


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
