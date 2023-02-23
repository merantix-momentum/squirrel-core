from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from _pytest.fixtures import SubRequest as Request
from pandas import DataFrame

from squirrel.catalog.catalog import CatalogSource
from squirrel.catalog.source import Source
from squirrel.constants import URL
from squirrel.driver import CsvDriver
from squirrel.driver.data_frame import ENGINE
from squirrel.iterstream import IterableSource


@pytest.fixture
def data_frame_ground_truth() -> DataFrame:
    """Create a DataFrame for testing."""
    # a b c
    # 1 10 100
    # 2 20 200
    # 3 30 300
    return DataFrame({"a": [1, 2, 3], "b": [10, 20, 30], "c": [100, 200, 300]})


@pytest.fixture(params=["csv", "excel", "feather", "parquet"])
def data_frame_source_path(
    request: Request, tmp_path: URL, data_frame_ground_truth: DataFrame
) -> tuple[str, URL, dict]:
    """Create a temporary file for all supported file types and write a temporary DataFrame to it.

    Returns:
        Returns the name of the driver, path to the temporary file, and read_kwargs.
    """

    df = data_frame_ground_truth
    name = request.param
    write_kwargs = {}
    read_kwargs_pandas = {}
    read_kwargs_dask = {}

    if name == "csv":
        ext = ".csv"
        write_fn = df.to_csv
        write_kwargs["index"] = False

    elif name == "excel":
        ext = ".xlsx"
        write_fn = df.to_excel
        write_kwargs["index"] = False

    elif name == "feather":
        ext = ".ft"
        write_fn = df.to_feather

    elif name == "json":
        ext = ".json"
        write_fn = df.to_json
        read_kwargs_dask["orient"] = "columns"

    elif name == "parquet":
        ext = ".pq"
        write_fn = df.to_parquet

    else:
        raise ValueError(f"Unknwon data frame driver name '{type}'.")

    # Write DataFrame to temporary path
    path = f"{tmp_path}/test.{ext}"
    write_fn(path, **write_kwargs)

    # Return name of used file type and path to temporary file
    return name, path, dict(pandas=read_kwargs_pandas, dask=read_kwargs_dask)


@pytest.fixture(params=["pandas", "dask"])
def engine(request: Request) -> ENGINE:
    """Data frame engines to use for testing."""
    return request.param


@pytest.fixture
def data_frame_source(data_frame_source_path: tuple[str, URL, dict], engine: ENGINE) -> tuple[Source, ENGINE]:
    """Get DataFrame source and engine for all drivers and used engines."""
    name, path, read_kwargs = data_frame_source_path
    read_kwargs = read_kwargs[engine]

    # Skip tests for drivers that do not support Dask
    if engine == "dask" and name in ["excel", "feather"]:
        pytest.skip("Dask loading not supported.")

    source = Source(name, driver_kwargs={"url": path, "engine": engine, "read_kwargs": read_kwargs})
    return source, engine


def test_dataframe_drivers(data_frame_source: tuple[Source, ENGINE], data_frame_ground_truth: DataFrame) -> None:
    """Test all DataFrameDrivers"""
    source, engine = data_frame_source
    driver = CatalogSource(source, source.driver_name, None).get_driver()
    df = driver.get_df()
    df_gt = data_frame_ground_truth

    if engine == "dask":
        df = df.compute()

    assert all(df_gt == df)

    for (row, series_gt), data in zip(df_gt.iterrows(), driver.get_iter()):
        d = data._asdict()
        del d["Index"]
        series = pd.Series(data=d, name=row)
        assert all(series_gt == series)


def test_dataframe_drivers_iterable_source(
    data_frame_source: tuple[Source, ENGINE], data_frame_ground_truth: DataFrame
) -> None:
    """Test async_map for all DataFrameDrivers"""

    source, engine = data_frame_source
    df_gt = data_frame_ground_truth

    # Calculate base summation value of DataFrame (first column)
    columns = df_gt.columns
    sum_base = df_gt[columns[0]].sum()

    # Define iterable sources
    n = 3
    sources = [source] * n

    # Create IterableSource and async_map calculate sum of columns
    iterable_source = IterableSource(sources)

    for i, col in enumerate(columns):
        res = (
            iterable_source.async_map(lambda x: CatalogSource(x, source.driver_name, None).get_driver().get_df())
            .async_map(lambda df: df.loc[:, col].sum())  # noqa
            .collect()
        )
        if engine == "dask":
            res = [val.compute() for val in res]

        assert res == [sum_base * 10**i] * n


def test_pandas_local_caching(data_frame_source, data_frame_ground_truth) -> None:
    source, engine = data_frame_source
    if engine != "pandas":
        pytest.skip("only pandas is supported with local_caching.")
    df_gt = data_frame_ground_truth

    with tempfile.TemporaryDirectory() as cach_dir:
        with tempfile.TemporaryDirectory() as data_dir:
            df_gt.to_csv(data_dir + "/test.csv", index=False)

            res = CsvDriver(data_dir + "/test.csv", local_caching=cach_dir + "/test.csv").get_df()
            assert res.sum().sum() == df_gt.sum().sum()
        assert not Path(data_dir + "/test.csv").exists()
        res_2 = CsvDriver(data_dir + "/test.csv", local_caching=cach_dir + "/test.csv").get_df()
        assert res_2.sum().sum() == df_gt.sum().sum()
