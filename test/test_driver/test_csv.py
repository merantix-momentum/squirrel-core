import pandas as pd
import pytest

from squirrel.constants import URL
from squirrel.driver import CsvDriver
from squirrel.iterstream import IterableSource


@pytest.fixture
def tmp_csv(tmp_path: URL) -> URL:
    """Create a csv file under `tmp_path`. No need to teardown, pytest will tear the entire tmp_path for you."""
    csv_path = f"{tmp_path}/test.csv"
    content = "a,b,c\n1,2,3"
    with open(csv_path, "w") as f:
        f.write(content)
    return csv_path


def test_csv(tmp_csv: URL) -> None:
    """Test reading csv"""
    df = CsvDriver(tmp_csv).get_df()
    assert df["a"].compute().iloc[0] == 1


def test_async_map_on_dataframe(tmp_csv: URL) -> None:
    """Test that async_map works when items are pandas dataframes."""
    n = 3
    csv_files = [tmp_csv] * n
    res = IterableSource(csv_files).async_map(pd.read_csv).async_map(lambda df: df.iloc[0, :].sum()).collect()
    assert res == [6] * n
