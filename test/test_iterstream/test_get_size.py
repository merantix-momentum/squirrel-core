from typing import Any, Dict, List

import pytest
from pytest import FixtureRequest

from squirrel.iterstream.iterators import getsize


def test_getsize(mock_object: Any) -> None:
    """Test function of getsize returns a positive number."""
    assert getsize(mock_object) > 0


@pytest.fixture
def mock_dictionary() -> Dict[int, str]:
    """A mock dictionary."""
    d = {}
    for i in range(10):
        d[i] = f"some/value/{i}"
    return d


@pytest.fixture
def mock_list() -> List[int]:
    """A mock list."""
    return [i for i in range(10)]


@pytest.fixture(params=["dict", "list", "int", "str"])
def mock_object(mock_dictionary: Dict[int, str], mock_list: List[int], request: FixtureRequest) -> Any:
    """Returns a mock dict, list, int or str."""
    if request.param == "dict":
        return mock_dictionary
    elif request.param == "list":
        return mock_list
    elif request.param == "int":
        return 1
    elif request.param == "str":
        return "some/string"
    return
