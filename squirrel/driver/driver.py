"""This module defines the Driver API of squirrel."""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional

from squirrel.catalog import Catalog
from squirrel.iterstream import IterableSource

if TYPE_CHECKING:
    from pandas import DataFrame

    from squirrel.iterstream import Composable

__all__ = ["Driver", "IterDriver", "MapDriver", "DataFrameDriver"]


class Driver(ABC):
    """Drives the access to a data source."""

    name: str

    def __init__(self, catalog: Optional[Catalog] = None, **kwargs) -> None:
        """Initializes driver with a catalog and arbitrary kwargs."""
        self._catalog = catalog if catalog is not None else Catalog()


class IterDriver(Driver):
    """A Driver that allows iteration over the items in the data source.

    Items can be iterated over using the :py:meth:`get_iter` method.
    """

    @abstractmethod
    def get_iter(self, **kwargs) -> Composable:
        """Returns an iterable of items in the form of a :py:class:`Composable`, which allows various stream
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

    def get_iter(
        self,
        keys_iterable: Optional[Iterable] = None,
        shuffle_key_buffer: int = 1,
        key_hooks: Optional[Iterable[Callable]] = None,
        max_workers: Optional[int] = None,
        prefetch_buffer: int = 10,
        shuffle_item_buffer: int = 1,
        flatten: bool = False,
        keys_kwargs: Optional[Dict] = None,
        get_kwargs: Optional[Dict] = None,
        key_shuffle_kwargs: Optional[Dict] = None,
        item_shuffle_kwargs: Optional[Dict] = None,
    ) -> Composable:
        """Returns an iterable of items in the form of a :py:class:`squirrel.iterstream.Composable`, which allows
        various stream manipulation functionalities.

        Items are fetched using the :py:meth:`get` method. The returned :py:class:`Composable` iterates over the items
        in the order of the keys returned by the :py:meth:`keys` method.

        Args:
            keys_iterable (Iterable, optional): If provided, only the keys in `keys_iterable` will be used to fetch
                items. If not provided, all keys in the store are used.
            shuffle_key_buffer (int): Size of the buffer used to shuffle keys.
            key_hooks (Iterable[Callable], optional): Functions that are applied to the keys before fetching the items.
            max_workers (int, Optional): If larger than 1 or None, :py:meth:`~squirrel.iterstream.Composable.async_map`
                is called to fetch multiple items simultaneously and `max_workers` refers to the maximum number of
                workers in the ThreadPoolExecutor used by `async_map`.
                Otherwise, :py:meth:`~squirrel.iterstream.Composable.map` is called and `max_workers` is not used.
                Defaults to None.
            prefetch_buffer (int): Size of the buffer used for prefetching items if `async_map` is used. See
                `max_workers` for more details. Please be aware of the memory footprint when setting this parameter.
            shuffle_item_buffer (int): Size of the buffer used to shuffle items after being fetched. Please be aware of
                the memory footprint when setting this parameter.
            flatten (bool): Whether to flatten the returned iterable. Defaults to False.
            keys_kwargs (Dict, optional): Keyword arguments passed to :py:meth:`keys` when getting the keys in the
                store. Not used if `keys_iterable` is provided. Defaults to None.
            get_kwargs (Dict, optional): Keyword arguments passed to :py:meth:`get` when fetching items. Defaults to
                None.
            key_shuffle_kwargs (Dict, optional): Keyword arguments passed to :py:meth:`shuffle` when shuffling keys.
                Defaults to None. Can be useful to e.g. set the seed etc.
            item_shuffle_kwargs (Dict, optional): Keyword arguments passed to :py:meth:`shuffle` when shuffling items.
                Defaults to None. Can be useful to e.g. set the seed etc.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the items in the store.
        """
        keys_kwargs = {} if keys_kwargs is None else keys_kwargs
        get_kwargs = {} if get_kwargs is None else get_kwargs
        key_shuffle_kwargs = {} if key_shuffle_kwargs is None else key_shuffle_kwargs
        item_shuffle_kwargs = {} if item_shuffle_kwargs is None else item_shuffle_kwargs
        keys_it = keys_iterable if keys_iterable is not None else self.keys(**keys_kwargs)
        it = IterableSource(keys_it).shuffle(size=shuffle_key_buffer, **key_shuffle_kwargs)

        if key_hooks:
            for hook in key_hooks:
                it = it.to(hook)

        map_fn = partial(self.get, **get_kwargs)
        _map = (
            it.map(map_fn)
            if max_workers is not None and max_workers <= 1
            else it.async_map(map_fn, prefetch_buffer, max_workers)
        )
        if flatten:
            _map = _map.flatten()

        return _map.shuffle(size=shuffle_item_buffer, **item_shuffle_kwargs)


class DataFrameDriver(Driver):
    @abstractmethod
    def get_df(self, **kwargs) -> DataFrame:
        """Returns a dataframe of the data."""