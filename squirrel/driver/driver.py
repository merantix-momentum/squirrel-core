"""This module defines the Driver API of squirrel."""

from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from functools import partial
from inspect import isclass
from typing import Any, Callable, Iterable, TYPE_CHECKING

from squirrel.catalog import Catalog
from squirrel.iterstream import Composable, IterableSource

if TYPE_CHECKING:
    from pandas import DataFrame


__all__ = ["Driver", "IterDriver", "MapDriver", "DataFrameDriver"]


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
        keys_iterable: Iterable | None = None,
        shuffle_key_buffer: int = 1,
        key_hooks: Iterable[Callable | type[Composable] | functools.partial] | None = None,
        max_workers: int | None = None,
        prefetch_buffer: int = 10,
        shuffle_item_buffer: int = 1,
        flatten: bool = False,
        keys_kwargs: dict | None = None,
        get_kwargs: dict | None = None,
        key_shuffle_kwargs: dict | None = None,
        item_shuffle_kwargs: dict | None = None,
    ) -> Composable:
        """Returns an iterable of items in the form of a :py:class:`squirrel.iterstream.Composable`, which allows
        various stream manipulation functionalities.

        Items are fetched using the :py:meth:`get` method. The returned :py:class:`Composable` iterates over the items
        in the order of the keys returned by the :py:meth:`keys` method.

        Args:
            keys_iterable (Iterable, optional): If provided, only the keys in `keys_iterable` will be used to fetch
                items. If not provided, all keys in the store are used.
            shuffle_key_buffer (int): Size of the buffer used to shuffle keys.
            key_hooks (Iterable[Iterable[Union[Callable, Type[Composable], functools.partial]]], optional): Hooks
                to apply to keys before fetching the items. It is an Iterable any of these objects:

                    1) subclass of :py:meth:`~squirrel.iterstream.Composable`: in this case, `.compose(hook, **kw)`
                       will be applied to the stream
                    2) A Callable:  `.to(hook, **kw)` will be applied to the stream
                    3) A partial function: the three attributes `args`, `keywords` and `func` will be retrieved, and
                       depending on whether `func` is a subclass of :py:meth:`~squirrel.iterstream.Composable` or a
                       `Callable`, one of the above cases will happen, with the only difference that arguments are
                       passed too. This is useful for passing arguments.
            max_workers (int, optional): If `max_workers` is equal to 0 or 1,
                :py:meth:`~squirrel.iterstream.Composable.map` is called to fetch the items iteratively.
                If `max_workers` is bigger than 1 or equal to `None`,
                :py:meth:`~squirrel.iterstream.Composable.async_map` is called to fetch multiple items simultaneously.
                In this case, the `max_workers` argument refers to the maximum number of
                workers in the :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>`
                Pool of :py:meth:`~squirrel.iterstream.Composable.async_map`.
                `None` has a special meaning in this context and uses an internal heuristic for the number of workers.
                The exact number of workers with `max_workers=None` depends on the specific Python version.
                See :py:class:`ThreadPoolExecutor <concurrent.futures.ThreadPoolExecutor>` for details.
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
        keys_it = keys_iterable if keys_iterable is not None else partial(self.keys, **keys_kwargs)
        it = IterableSource(keys_it).shuffle(size=shuffle_key_buffer, **key_shuffle_kwargs)

        if key_hooks:
            for hook in key_hooks:
                arg = []
                kwarg = {}
                f = hook
                if isinstance(hook, partial):
                    arg = hook.args
                    kwarg = hook.keywords
                    f = hook.func

                if isclass(f) and issubclass(f, Composable):
                    it = it.compose(f, *arg, **kwarg)
                elif isinstance(f, Callable):
                    it = it.to(f, *arg, **kwarg)
                else:
                    raise ValueError(
                        f"wrong argument for hook {hook}, it should be a Callable, partial function, or a subclass "
                        f"of Composable"
                    )

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
