from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import more_itertools

from squirrel.driver.csv_driver import DataFrameDriver
from squirrel.driver.driver import MapDriver
from squirrel.iterstream.source import IterableSamplerSource, IterableSource

if TYPE_CHECKING:
    import random

    from dask.dataframe import DataFrame

    from squirrel.catalog import Catalog, CatalogKey
    from squirrel.catalog.source import Source
    from squirrel.iterstream import Composable
    from squirrel.store import AbstractStore


class SourceCombiner(MapDriver, DataFrameDriver):
    name = "source_combiner"

    def __init__(self, subsets: Dict[str, CatalogKey], catalog: Catalog, **kwargs) -> None:
        """Initializes SourceCombiner.

        Args:
            subsets (Dict[str, CatalogKey]): Keys define the names of the subsets, values are tuples of the
                corresponding (catalog entry, version) combinations.
            catalog (Catalog): The parent catalog which the subset sources are part of.
            **kwargs: Keyword arguments to be passed to the super class.
        """
        super().__init__(catalog=catalog, **kwargs)
        self._subsets = subsets

    @property
    def subsets(self) -> List[str]:
        """Ids of all subsets defined by this source."""
        return list(self._subsets.keys())

    def get_source(self, subset: str) -> Source:
        """Returns subset source based on subset id.

        Args:
            subset (str): Id of subset in this source definition.

        Returns:
            (Source): Subset source.
        """
        key, version = self._subsets[subset]
        return self._catalog[key][version]

    def get_iter(self, subset: Optional[str] = None, **kwargs) -> Composable:
        """Routes to the :py:meth:`get_iter` method of the appropriate subset driver.

        Args:
            subset (str): Id of the subset in this source definition. If None, interleaves iterables obtained from all
                subset drivers.
            **kwargs: Keyword arguments passed to the subset driver.

        Returns:
            (Composable) Iterable over the items of subset driver(s) in the form of a :py:class:`Composable`.
        """
        if subset is None:
            return IterableSource(
                more_itertools.interleave_longest(*[self.get_iter(subset=k, **kwargs) for k in self.subsets])
            )
        return self.get_source(subset).get_driver().get_iter(**kwargs)

    def get(self, subset: str, key: Any, **kwargs) -> Iterable:
        """Routes to the :py:meth:`get` method of the appropriate subset driver.

        Args:
            subset (str): Id of the subset in this source definition.
            key (str): Key of the item to get.
            **kwargs: Keyword arguments passed to the subset driver.

        Returns:
            (Iterable) Iterable over the items corresponding to `key` for subset driver `subset`.
        """
        return self.get_source(subset).get_driver().get(key, **kwargs)

    def keys(self, subset: str, **kwargs) -> Iterable:
        """Routes to the :py:meth:`keys` method of the appropriate subset driver.

        Args:
            subset (str): Id of the subset in this source definition.
            **kwargs: Keyword arguments passed to the subset driver.

        Returns:
            (Iterable) Iterable over the keys for subset driver `subset`.
        """
        return self.get_source(subset).get_driver().keys(**kwargs)

    def get_df(self, subset: str, **kwargs) -> DataFrame:
        """Routes to the :py:meth:`get_df` method of the appropriate subset driver.

        Args:
            subset (str): Id of the subset in this source definition.
            **kwargs: Keyword arguments passed to the subset driver.

        Returns:
            (DataFrame) Data of the subset driver `subset` as a Dask DataFrame.
        """
        return self.get_source(subset).get_driver().get_df(*kwargs)

    def get_store(self, subset: str) -> AbstractStore:
        """Returns the store of the appropriate subset driver.

        Args:
            subset (str): Id of the subset in this source definition.

        Returns:
            (AbstractStore) Store of the subset driver `subset`.
        """
        return self.get_source(subset).get_driver().store

    def get_iter_sampler(
        self,
        probs: Optional[List[float]] = None,
        rng: Optional[random.Random] = None,
        seed: Optional[int] = None,
        **kwargs,
    ) -> Composable:
        """Returns an iterstream that samples from the subsets of this source.

        Args:
            rng (random.Random): A random number generator.
            probs (List[float]): List of probabilities to sample from the subsets. If None, sample uniform.
            **kwargs: Keyword arguments passed to the :py:meth:`get_iter` method of each subset driver.

        Returns:
            (Composable) Iterable over samples randomly sampled from subsets.
        """
        return IterableSamplerSource(
            iterables=[self.get_iter(subset=k, **kwargs) for k in self.subsets], probs=probs, rng=rng, seed=seed
        )
