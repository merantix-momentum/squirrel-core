import pytest

from squirrel.benchmark.quantify_randomness import quantify_randomness


@pytest.mark.parametrize("shards", [5, 10])
@pytest.mark.parametrize("shard_size", [5, 10])
def test_async_map_on_dataframe(shards: int, shard_size: int) -> None:
    """Test that shuffled sequences produce more randomness."""
    ktau_deterministic = quantify_randomness(shards, shard_size, 1, 1, n_samples=100)
    ktau_fully_random = quantify_randomness(shards, shard_size, shard_size, shard_size, n_samples=100)

    assert 1.0 > ktau_deterministic > ktau_fully_random > 0.0
