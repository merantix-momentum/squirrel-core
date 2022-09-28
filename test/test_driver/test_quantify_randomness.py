import pytest

from squirrel.driver.quantify_randomness import quantify_randomness


@pytest.mark.parametrize("shards", [5, 10])
@pytest.mark.parametrize("shard_size", [5, 10])
def test_async_map_on_dataframe(shards: int, shard_size: int) -> None:
    """Test that shuffled sequences produce more randomness."""
    ktau_deterministic = quantify_randomness(shards, shard_size, 1, 1)
    ktau_quasi_random = quantify_randomness(shards, shard_size, shard_size // 2, shard_size // 2)
    ktau_fully_random = quantify_randomness(shards, shard_size, shard_size, shard_size)
    
    assert ktau_deterministic > ktau_quasi_random > ktau_fully_random
