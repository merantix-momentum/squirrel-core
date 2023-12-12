"""Test module for the data multiplexing."""
import pickle
import string
from collections import Counter

import hypothesis.strategies as st
import numpy as np
import pytest
from unittest import mock
from hypothesis import given, settings
from squirrel.catalog import Catalog
from squirrel.driver import IterDriver
from squirrel.iterstream import IterableSource, Multiplexer, MultiplexingStrategy


@pytest.mark.parametrize(
    "mux_strategy",
    [MultiplexingStrategy.ROUND_ROBIN, MultiplexingStrategy.UNIFORM_WITH_REPLACEMENT],
)
def test_multiplexer(mux_strategy: MultiplexingStrategy) -> None:
    """Testing the construction of the multiplexer."""
    it_0 = IterableSource([f"{cnt:02d}" for cnt in range(100)])
    it_1 = IterableSource(list(string.ascii_lowercase))
    it_2 = IterableSource([f"{cnt:02d}" for cnt in range(1000, 1200)])

    mux = Multiplexer([it_0, it_1, it_2], mux_strategy=mux_strategy, seed=42)

    if mux_strategy == MultiplexingStrategy.ROUND_ROBIN:
        res = mux.collect()
        assert len(res) == 100 + 26 + 200
    if mux_strategy == MultiplexingStrategy.UNIFORM_WITH_REPLACEMENT:
        res = mux.collect()
        assert len(res) == 579
        assert mux.reinit_counts == [1, 7, 1]


@pytest.mark.parametrize(
    "mux_strategy",
    [MultiplexingStrategy.ROUND_ROBIN, MultiplexingStrategy.UNIFORM_WITH_REPLACEMENT],
)
def test_dummy_data(dummy_data_catalog: Catalog, mux_strategy: MultiplexingStrategy) -> None:
    """Test multiplexer on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    mux = Multiplexer([d0.get_iter(), d1.get_iter(), d2.get_iter()], mux_strategy=mux_strategy)

    result = mux.map(lambda x: x["meta"]["sha"]).collect()
    cntr = Counter(result)
    cntr = dict(sorted(cntr.items(), key=lambda x: x[1]))
    assert min(cntr.values()) == 1


def test_round_robin(dummy_data_catalog: Catalog) -> None:
    """Test multiplexer on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    d0_count = cat["data_0"].metadata["num_samples"]
    d1_count = cat["data_1"].metadata["num_samples"]
    d2_count = cat["data_2"].metadata["num_samples"]

    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.ROUND_ROBIN,
    )

    result = mux.map(lambda x: x["meta"]["dataset"]).collect()
    assert len(result) == sum([d0_count, d1_count, d2_count])
    cntr = Counter(result)
    assert cntr["data_0"] == d0_count
    assert cntr["data_1"] == d1_count
    assert cntr["data_2"] == d2_count


@given(
    st.integers(min_value=42, max_value=1001),
    st.floats(5e-3, 0.9),
    st.floats(5e-2, 0.09),
)
@settings(max_examples=5, deadline=2000)
def test_sampling_ratio(dummy_data_catalog: Catalog, seed: int, p0: float, p1: float) -> None:
    """Test multiplexer on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    d0_count = cat["data_0"].metadata["num_samples"]
    d1_count = cat["data_1"].metadata["num_samples"]
    d2_count = cat["data_2"].metadata["num_samples"]

    p = [p0, p1, 1.0 - p0 - p1]
    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.SAMPLING,
        sampling_probas=p,
        seed=seed,
    )

    result = mux.map(lambda x: x["meta"]["dataset"]).collect()
    cntr = Counter(result)
    cntr = dict(sorted(cntr.items(), key=lambda x: x[0]))

    assert any(np.asarray([d0_count, d1_count, d2_count]) == np.asarray(list(cntr.values())))
    np.testing.assert_allclose(np.asarray(list(cntr.values())) / len(result), p, atol=2.5e-1)


def test_oversampling_limit(dummy_data_catalog: Catalog) -> None:
    """Test multiplexer on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    p = [0.35, 0.6, 0.05]
    max_reinits = 4
    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.SAMPLING,
        sampling_probas=p,
        seed=42,
        max_reinits=max_reinits,
    )

    result = mux.map(lambda x: x["meta"]["dataset"]).collect()
    cntr = Counter(result)
    cntr = dict(sorted(cntr.items(), key=lambda x: x[0]))

    assert mux.reinit_counts == [1, 4, 0]
    assert mux.num_samples == [268, 460, 40]

    assert any(np.asarray([268, 460, 40]) == np.asarray(list(cntr.values())))
    np.testing.assert_allclose(np.asarray(list(cntr.values())) / len(result), p, atol=2.5e-1)

    np.testing.assert_allclose(np.asarray(mux.num_samples) / np.asarray(mux.num_samples).sum(), p, atol=2.5e-1)


def test_muxer_is_picklable(dummy_data_catalog: Catalog) -> None:
    """Test multiplexer is in fact picklable."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    p = [0.35, 0.6, 0.05]
    max_reinits = 4
    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.SAMPLING,
        sampling_probas=p,
        seed=42,
        max_reinits=max_reinits,
    )

    _bytes = pickle.dumps(mux)
    unpickle = pickle.loads(_bytes)

    result = unpickle.map(lambda x: x["meta"]["dataset"]).collect()
    cntr = Counter(result)
    cntr = dict(sorted(cntr.items(), key=lambda x: x[0]))

    assert unpickle.reinit_counts == [1, 4, 0]

    assert any(np.asarray([268, 460, 40]) == np.asarray(list(cntr.values())))
    np.testing.assert_allclose(np.asarray(list(cntr.values())) / len(result), p, atol=2.5e-1)


@pytest.mark.parametrize("num_workers, seed", [(2, 42), (5, 57), (6, 100)])
def test_round_robin_with_dataloader(dummy_data_catalog: Catalog, num_workers: int, seed: int) -> None:
    """Test multiplexer with dataloader on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    d0_count = cat["data_0"].metadata["num_samples"]
    d1_count = cat["data_1"].metadata["num_samples"]
    d2_count = cat["data_2"].metadata["num_samples"]

    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.ROUND_ROBIN,
        seed=seed,
    )
    datasets = mux.split_by_rank_pytorch().split_by_worker_pytorch().to_torch_iterable()

    import torch.utils.data as tud

    dl = tud.DataLoader(
        datasets,
        batch_size=None,
        num_workers=num_workers,
        multiprocessing_context="forkserver" if num_workers > 0 else None,
    )

    cntr: dict[str, int] = {}
    for _, record in enumerate(dl):
        assert len(record) == 2
        key = record["meta"]["dataset"]
        cntr[key] = cntr.get(key, 0) + 1

    assert cntr["data_0"] == d0_count
    assert cntr["data_1"] == d1_count
    assert cntr["data_2"] == d2_count


@pytest.mark.parametrize("num_workers, max_reinits, seed", [(2, 8, 42), (5, 10, 57), (6, 12, 100)])
def test_sampling_ratio_with_dataloader(
    dummy_data_catalog: Catalog, num_workers: int, max_reinits: int, seed: int
) -> None:
    """Test multiplexer with dataloader on fake catalog + dataset setting."""
    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    d0_count = cat["data_0"].metadata["num_samples"]
    d1_count = cat["data_1"].metadata["num_samples"]
    d2_count = cat["data_2"].metadata["num_samples"]

    p = [0.35, 0.6, 0.05]
    mux = Multiplexer(
        [d0.get_iter(), d1.get_iter(), d2.get_iter()],
        mux_strategy=MultiplexingStrategy.SAMPLING,
        sampling_probas=p,
        seed=seed,
        max_reinits=max_reinits,
    )
    datasets = mux.split_by_rank_pytorch().split_by_worker_pytorch().to_torch_iterable()

    import torch.utils.data as tud

    dl = tud.DataLoader(
        datasets,
        batch_size=None,
        num_workers=num_workers,
        multiprocessing_context="forkserver" if num_workers > 0 else None,
    )

    cntr: dict[str, int] = {}
    for _, record in enumerate(dl):
        assert len(record) == 2
        key = record["meta"]["dataset"]
        cntr[key] = cntr.get(key, 0) + 1
    cntr = dict(sorted(cntr.items(), key=lambda x: x[0]))
    reinits_counter = np.asarray(list(cntr.values())) / np.asarray([d0_count, d1_count, d2_count])

    assert reinits_counter.max() == max_reinits or reinits_counter.min() == 1
    np.testing.assert_allclose(np.asarray(list(cntr.values())) / sum(list(cntr.values())), p, atol=1e-2)


@mock.patch("torch.distributed.is_available", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.is_initialized", mock.MagicMock(return_value=True))
@mock.patch("torch.distributed.get_world_size")
@mock.patch("torch.distributed.get_rank")
@pytest.mark.parametrize("world_size", [1, 2, 4])
def test_multi_rank_multi_worker_with_muxing(
    mock_get_rank: int, mock_get_world_size: int, dummy_data_catalog: Catalog, world_size: int
) -> None:
    """Test muxing in multi-rank works reliably."""
    mock_get_world_size.return_value = world_size

    cat = dummy_data_catalog
    d0: IterDriver = cat["data_0"].get_driver()
    d1: IterDriver = cat["data_1"].get_driver()
    d2: IterDriver = cat["data_2"].get_driver()

    d0_count = cat["data_0"].metadata["num_samples"]
    d1_count = cat["data_1"].metadata["num_samples"]
    d2_count = cat["data_2"].metadata["num_samples"]

    rank_items = {}

    for rank in range(world_size):
        mock_get_rank.return_value = rank
        cat = dummy_data_catalog
        d0: IterDriver = cat["data_0"].get_driver()
        d1: IterDriver = cat["data_1"].get_driver()
        d2: IterDriver = cat["data_2"].get_driver()
        p = [0.35, 0.6, 0.05]
        max_reinits = 4
        mux = Multiplexer(
            [
                d0.get_iter(),
                d1.get_iter(),
                d2.get_iter(),
            ],
            mux_strategy=MultiplexingStrategy.ROUND_ROBIN,
            sampling_probas=p,
            seed=42,
            max_reinits=max_reinits,
        )

        rank_items[rank] = mux.map(lambda x: x["meta"]["sha"]).collect()

    actual = []
    for rank in range(world_size):
        v = rank_items[rank]
        actual = actual + v

    assert len(set(actual)) == d0_count + d1_count + d2_count
