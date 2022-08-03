from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List

import numpy as np
from squirrel.iterstream import Composable


def default_dict_collate(records: List[Dict[str, Any]]) -> List[np.ndarray]:
    """Converts a batch of a list of dicts to a list of numpy arrays. This converts your data from e.g.
        [{"img": [...], "label": 0}, {"img": [...], "label": 1}, ...] into
        [np.array(np.array([...]), np.array([...])), np.array([np.array(0),np.array(1)])].

        Leaving this here, because this function is useful for turning Squirrel pipelines that fetch dicts
        into a Nvidia DALI external source that accepts a list of numpy arrays.

    Args:
        records (List[Dict[str, Any]]): A batch to convert.

    Returns:
        List[np.ndarray]: Re-formatted batch of numpy arrays.
    """
    batch = defaultdict(list)
    for elem in records:
        for k, v in elem.items():
            batch[k].append(np.array(v))
    return [np.array(v) for v in batch.values()]


class DaliExternalSource(Composable):
    """Mixin-Composable to turn the Squirrel iterator into a DALI external source."""

    def __init__(self, batch_size: int, collation_fn: Callable[[List], List]) -> None:
        """Initializes the DaliExternalSource.

        Args:
            batch_size (int): The number of items in your batch.
            collation_fn (Callable[[Any], List]): The collation function to batch a list of items.
                DALI expects your data as a list of numpy or cupy arrays.
        """
        super().__init__()
        self.collation_fn = collation_fn
        self.batch_size = batch_size

    def __iter__(self) -> DaliExternalSource:
        """Returns the DaliExternalSource. Also initializes the iterator.

        Returns:
            DaliExternalSource: The iterator object.
        """
        self.it = iter(
            self.source.batched(
                self.batch_size,
                collation_fn=self.collation_fn,
                drop_last_if_not_full=False,
            )
        )
        return self

    def __next__(self) -> List[np.ndarray]:
        """Returns the next item of the iterator.

        Returns:
            List[np.ndarray]: The next batch of your dataset.
        """
        return next(self.it)
