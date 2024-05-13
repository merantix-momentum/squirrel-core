"""This module defines the Driver API of squirrel."""

from __future__ import annotations

import ray
from ray.data import Dataset
import functools
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable

from squirrel.catalog import Catalog


__all__ = ["Driver", "IterDriver", "MapDriver"]


class Driver(ABC):  # noqa: B024, we want to make it explicit that this class is abstract
    """Drives the access to a data source."""

    name: str

    def __init__(self, catalog: Catalog | None = None, **kwargs) -> None:
        """Initializes driver with a catalog and arbitrary kwargs."""
        self._catalog = catalog if catalog is not None else Catalog()


class IterDriver(Driver):
    """A Driver that allows iteration over the items in the data source.

    Items can be iterated over using the :py:meth:`get_iter` method.
    """

    @abstractmethod
    def get_iter(self, **kwargs) -> Dataset:
        """Returns an iterable of items in the form of a :py:class:`Dataset`, which allows various stream
        manipulation functionalities.

        The order of the items in the iterable may or may not be randomized, depending on the implementation and
        `kwargs`.
        """


class MapDriver(IterDriver):
    """A Driver that allows retrieval of items using keys, in addition to allowing iteration over the items."""

    @abstractmethod
    def get(self, key: Any, **kwargs) -> Any:
        """Returns an iterable over the items corresponding to `key`.

        Note that it is possible to implement this method according to your needs. There is no restriction on the type
        or number of items. For example, a key might be corresponding to a single item that holds a single sample or to
        a single item that contains one shard of multiple samples.

        If the method returns a single sample, then the :py:meth:`get_iter` method should be called with
        `flatten=False` since the stream does not need to be flattened. Otherwise, e.g. if the method returns an
        iterable of samples, then the :py:meth:`get_iter` method should be called with `flatten=True` if it desirable
        to have individual samples in the iterstream.
        """

    @abstractmethod
    def keys(self, **kwargs) -> Iterable:
        """Returns an iterable of the keys for the objects that are obtainable through the driver."""

    def get_iter(self,
        keys_iterable: Iterable | None = None,
        keys_kwargs: dict | None = None,
        get_kwargs: dict | None = None,
    ) -> Dataset:
        """Returns a Ray Dataset containing the items."""

        keys = self.keys(**keys_kwargs) if keys_iterable is None else keys_iterable

        items = [self.get(key, **get_kwargs) for key in keys]

        dataset = ray.data.from_items(items)
