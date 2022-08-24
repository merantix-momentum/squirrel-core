import random
import typing as t
from concurrent.futures import ThreadPoolExecutor

from squirrel.fsspec.fs import get_fs_from_url, get_protocol
from squirrel.iterstream.base import AsyncContent, Composable
from squirrel.iterstream.iterators import get_random_range

__all__ = ["IterableSource", "FilePathGenerator", "IterableSamplerSource"]


class IterableSource(Composable):
    """A class that turns an iterable to a source of a stream and provides stream manipulation functionalities on top,
    for instance:
    - map
    - map_async
    - filter
    - batched
    - shuffle
    - and more

    For the detailed description of each, please refer to the corresponding docstring in :py:class:`Composable`.
    """

    def __init__(self, source: t.Optional[t.Union[t.Iterable, t.Callable]] = ()):
        """Initialize IterableSource.

        Args:
            source (Union[Iterable, Callable], Optional): An Iterable that the IterableSource is
                built based on, or a callable that generates items when called.
        """
        super().__init__(source=source)

    def __iter__(self) -> t.Iterator:
        """Iterates over the items in the iterable"""
        if isinstance(self.source, t.Callable):
            yield from self.source()
        else:
            yield from self.source


class FilePathGenerator(Composable):
    """
    A specialized version of `Composable` that accepts a url without instantiating a filesystem instance in the init.
    It simply generates directories under the given `url` by instantiating a fsspec filesystem and yielding the result
    of fs.ls(url).
    """

    def __init__(
        self,
        url: str,
        nested: bool = False,
        max_workers: t.Optional[int] = None,
        max_keys: int = 1_000_000,
        max_dirs: int = 10,
        **storage_options,
    ):
        """
        Args:
            url: the url for which, ls is performed
            nested: if True, it attempts to make ls on each directory that it encounters. Otherwise, it will only yields
                the top-level paths and will not expand if the path is a directory
            max_workers (int): passed to the ThreadPoolExecutor. Only applicable if nested==True
            max_keys (int): maximum number of keys to keep in memory at the same time. If this number is reached, no new
                expansion on the currently discovered directories is done, until enough keys are yielded to make room
                for the new ones.
            max_dirs (int): maximum number of parallel ls operation.
            **storage_options (dict): kwargs to pass onto the fsspec filesystem initialization.
        """
        super().__init__()
        self.url = url
        self.protocol = get_protocol(self.url)
        self.nested = nested
        self.max_workers = max_workers
        self.max_keys = max_keys
        self.max_dirs = max_dirs
        self.storage_options = storage_options

    def __iter__(self) -> t.Iterator[str]:
        """Iterator that does ls and yield filepaths under the given url"""
        self.fs = get_fs_from_url(self.url, **self.storage_options)
        urls = self.fs.ls(self.url) if self.fs.exists(self.url) else []
        urls.sort()
        if self.nested:
            dirs = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                while len(urls) > 0 or dirs:
                    if len(urls) > 0:
                        url = urls.pop()
                        if self.fs.isdir(path=url):
                            future = AsyncContent(url, self.fs.ls, pool)
                            dirs.append(future)
                        else:
                            yield f"{self.protocol}{url}"
                    if (len(dirs) >= self.max_dirs and len(urls) < self.max_keys) or len(urls) == 0 and dirs:
                        d = dirs.pop(0).value()
                        urls.extend(d)
        else:
            for url in urls:
                yield f"{self.protocol}{url}"


class IterableSamplerSource(Composable):
    """A class that samples from iterables into an iterstream."""

    def __init__(
        self,
        iterables: t.List[t.Iterable],
        probs: t.Optional[t.List[float]] = None,
        rng: t.Optional[random.Random] = None,
        seed: t.Optional[int] = None,
    ):
        """Initialize IterableSamplerSource.

        Args:
            iterables (List[Iterable]): List of iterables to sample from.
            probs (Optional[List[float]], optional): [description]. Defaults to None.
            rng (random.Random, optional): Random number generator to use.
            seed (Optional[int]): An int or other acceptable types that works for random.seed(). Will be used to seed
                `rng`. If None, a unique identifier will be used to seed.
        """
        super().__init__(source=())
        self.rng = get_random_range(rng, seed)
        self.iterators = [iter(it) for it in iterables]
        if probs is not None:
            assert len(probs) == len(self.iterators), "number of iterables and probs must be equal"
            assert sum(probs) == 1, "sum of probs must add up to 1"
            assert all(p > 0 for p in probs), "probability for each iterable must be positive"
        self.probs = probs

    def __iter__(self) -> t.Iterator:
        """Samples items from the iterables, returns all samples until all iterables are exhausted."""
        while True:
            idx = self.rng.choices(range(len(self.iterators)), weights=self.probs)[0]
            try:
                yield next(self.iterators[idx])
            except StopIteration:
                self.iterators = self.iterators[:idx] + self.iterators[idx + 1 :]
                if self.probs is not None:
                    probs = self.probs[:idx] + self.probs[idx + 1 :]
                    s = sum(probs)
                    self.probs = [p / s for p in probs]
            if len(self.iterators) == 0:
                break
