"""This module defines functions that should be importable for integration tests and unit tests as well.

We use a separate module from conftest.py to circumvent imports from conftest, since this name can be ambiguous
when multiple conftest.py exist (e.g. for unit and integration tests) in one run.
Not importing from conftest is a best practice described in the note here:
https://pytest.org/en/6.2.x/writing_plugins.html#conftest-py-local-per-directory-plugins

This means we only use conftest.py to define fixtures which will automatically be shared with the respective scope.
"""
from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Tuple
from unittest import mock
from unittest.mock import MagicMock

import numpy as np

if TYPE_CHECKING:
    from squirrel.constants import SampleType


SHAPE = Tuple[int, int, int]


def create_torch_mock(t_worker: Tuple[int, int], t_world: Tuple[int, int]) -> MagicMock:
    """Returns a mock of the pytorch module with parallel and distributed worker."""
    torch_mock = mock.MagicMock()
    torch_mock.distributed.is_available.return_value = True
    torch_mock.distributed.is_initialized.return_value = True
    torch_mock.distributed.get_rank.return_value = t_world[0]
    torch_mock.distributed.get_world_size.return_value = t_world[1]
    torch_mock.utils.data.get_worker_info.return_value = collections.namedtuple("workerinfo", "id num_workers")(
        t_worker[0], t_worker[1]
    )
    return torch_mock


def get_sample(array_shape: SHAPE) -> SampleType:
    """Generate a single sample."""
    return {
        "key": f"_{np.random.randint(1, 10000)}",
        "image": np.random.random(size=array_shape),
        "label": np.random.choice([0, 1]),
    }
