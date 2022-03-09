"""This module defines the Store API of squirrel."""

import typing as t
from abc import ABC, abstractmethod


class AbstractStore(ABC):
    """Abstract class that specifies the map-style storage api of squirrel.

    A store is responsible for persisting and retrieving back items. Each item is persisted with a corresponding key
    using the :py:meth:`set` method. Then, the item can be retrieved using the :py:meth:`get` method. Keys to all
    persisted items can be retrieved using the :py:meth:`keys` method.

    The meaning or content of a single item depends on the use case and the implementation of the store. For most
    machine-learning-related tasks, an item would be a single sample, or a batch or shard of samples.
    """

    @abstractmethod
    def set(self, key: t.Any, value: t.Any, **kwargs) -> None:
        """Persists an item with the given key.

        There is no restriction on the type of the item. However, implementations of :py:class:`AbstractStore` can put
        their own restrictions.
        """

    @abstractmethod
    def get(self, key: t.Any, **kwargs) -> t.Iterable:
        """Returns an iterable over the item(s) corresponding to the given key.

        Note that it is possible to implement this method as a generator, or to simply return a list of items. There is
        no restriction on the type or number of items. For example, a key might be corresponding to a single item that
        holds a single sample or to a single item that contains one shard of multiple samples.
        """

    @abstractmethod
    def keys(self, **kwargs) -> t.Iterable:
        """Returns an iterable over all keys in the store.

        Note that it is possible to implement this method as a generator, or to simply return a list of keys. The
        iterable should contain the keys for all items retrievable using :py:meth:`get`.
        """
