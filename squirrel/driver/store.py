from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterable

from squirrel.driver.driver import MapDriver
from squirrel.iterstream.source import IterableSource
from squirrel.serialization import SquirrelSerializer, ParquetSerializer
from squirrel.store import SquirrelStore

if TYPE_CHECKING:
    from squirrel.iterstream import Composable
    from squirrel.store.store import AbstractStore
    from ray.data import Dataset


class StoreDriver(MapDriver):
    """A :py:class`MapDriver` implementation, which uses an :py:class`AbstractStore` instance to retrieve its items.

    The store used by the driver can be accessed via the :py:property:`store` property.
    """

    name = "store_driver"

    def __init__(
        self,
        url: str,
        serializer: SquirrelSerializer,
        storage_options: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Initializes StoreDriver.

        Args:
            url (str): the url of the store
            serializer (SquirrelSerializer): serializer to be passed to SquirrelStore
            storage_options (Optional[Dict[str, Any]]): a dict with keyword arguments to be passed to store initializer
                Example of storage_options if you want to enable `fsspec` caching:
                `storage_options={"protocol": "simplecache", "target_protocol": "gs", "cache_storage": "path/to/cache"}`
            **kwargs: Keyword arguments to pass to the super class initializer.
        """
        super().__init__(**kwargs)
        self.url = url
        self.serializer = serializer
        self.storage_options = storage_options if storage_options is not None else {}
        self._store = None

    def get(self, key: Any, **kwargs) -> Iterable:
        """Returns an iterable over the items corresponding to `key` using the store instance.

        Calls and returns the result of :py:meth:`self.store.get`. Subclasses might filter or manipulate the iterable
        over items returned from the store.

        Args:
            key (Any): Key with which the items will be retrieved. Must be of type and format that is supported by the
            store instance.
            **kwargs: Keyword arguments passed to the :py:meth:`self.store.get` method.

        Returns:
            (Iterable) Iterable over the items corresponding to `key`, as returned from the store.
        """
        return self.store.get(key, **kwargs)

    def keys(self, **kwargs) -> Iterable:
        """Returns an iterable over all keys to the items that are obtainable through the driver.

        Calls and returns the result of :py:meth:`self.store.keys`. Subclasses might filter or manipulate the iterable
        over keys returned from the store.

        Args:
            **kwargs: Keyword arguments passed to the :py:meth:`self.store.keys` method.

        Returns:
            (Iterable) Iterable over all keys in the store, as returned from the store.
        """
        return self.store.keys(**kwargs)

    @property
    def store(self) -> AbstractStore:
        """Store that is used by the driver."""
        if self._store is None:
            self._store = SquirrelStore(url=self.url, serializer=self.serializer, **self.storage_options)
        return self._store

    def get_iter(self, flatten: bool = True, **kwargs) -> Composable:
        """Returns an iterable of items in the form of a :py:class:`squirrel.iterstream.Composable`, which allows
        various stream manipulation functionalities.

        Items are fetched using the :py:meth:`get` method. The returned :py:class:`Composable` iterates over the items
        in the order of the keys returned by the :py:meth:`keys` method.

        Args:
            flatten (bool): Whether to flatten the returned iterable. Defaults to True.
            **kwargs: Other keyword arguments passed to `super().get_iter()`. For details, see
                :py:meth:`squirrel.driver.MapDriver.get_iter`.

        Returns:
            (squirrel.iterstream.Composable) Iterable over the items in the store.
        """
        return super().get_iter(flatten=flatten, **kwargs)

    def get_iter_ray(
        self,
        *,
        prefetch_buffer: int = 10,
        local_shuffle_buffer_size: int = 10,
        shuffle_item_buffer: int = 1,
        item_shuffle_kwargs: dict | None = None,
        drop_last: bool = False,
        batch_size: int = 100,
        collate_fn: Callable = None,
        local_shuffle_seed: int = 42,
    ) -> None:
        """Get a Composable that wraps `ray.data.DataIterator.iter_batches`.

        Args:
            prefetch_buffer (int): passed to `ray.data.DataIterator.iter_batches`.
            local_shuffle_buffer_size (int): passed to `ray.data.DataIterator.iter_batches`.
            shuffle_item_buffer (int): shuffle the individual samples using squirrel's Composable.shuffle()
            item_shuffle_kwargs (int): kwargs passed to squirrel's Composable.shuffle()
            drop_last (bool): passed to `ray.data.DataIterator.iter_batches`.
            batch_size(int): passed to `ray.data.DataIterator.iter_batches`.
            collate_fn (Callable): a function passed to `ray.data.DataIterator.iter_batches`. It is applied to the batch
                in in the pydict fashion, i.e. {"col_1": [1, 2, 3], "col_2": [4, 5, 6]}, and must return the result in
                this format as well.
            local_shuffle_seed (int): passed to `ray.data.DataIterator.iter_batches`.
        """
        if item_shuffle_kwargs is None:
            item_shuffle_kwargs = {}

        ds_iter_batch = self.get_ray_iter_batch(
            prefetch_buffer=prefetch_buffer,
            batch_format="numpy",
            drop_last=drop_last,
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            collate_fn=collate_fn,
        )

        it = IterableSource(ds_iter_batch)
        if isinstance(self.serializer, ParquetSerializer):
            import pyarrow as pa

            # TODO: check the performance of the line below and maybe change
            it = it.map(lambda x: pa.RecordBatch.to_pylist(pa.RecordBatch.from_pydict(x))).flatten()
        return it.shuffle(size=shuffle_item_buffer, **item_shuffle_kwargs)

    def get_ray_dataset(self) -> Dataset:
        """Get a ray dataset"""
        import ray

        if not isinstance(self.serializer, ParquetSerializer):
            raise NotImplementedError(f"serializer {self.serializer} does not support Ray")
        ds = ray.data
        ds = ds.read_parquet(self.url)
        return ds

    def get_ray_iter_batch(
        self,
        prefetch_buffer: int = 10,
        batch_format: str = "numpy",
        local_shuffle_buffer_size: int = 1,
        local_shuffle_seed: int = 42,
        drop_last: bool = False,
        batch_size: int = 100,
        collate_fn: Callable = None,
        **kwargs: dict | None,
    ) -> Iterable:
        """Get a ray iter_batch"""
        ds = self.get_ray_dataset()
        return ds.iter_batches(
            prefetch_batches=prefetch_buffer,
            batch_format=batch_format,
            drop_last=drop_last,
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            _collate_fn=collate_fn,
            **kwargs,
        )
