"""Multiplexing module to combine and sample from a set of composables."""
from __future__ import annotations

import itertools
import logging
import sys
import typing as t
from dataclasses import dataclass
from enum import Enum

import numpy as np
from squirrel.iterstream import Composable

logger = logging.getLogger(__name__)


@dataclass
class MuxIterator:
    """Convenience dataclass to keep track of iterator position and reinits."""

    index: int
    it: t.Iterator
    reinits: int


class MultiplexingStrategy(Enum):
    """Enum of possible multiplexing strategies."""

    ROUND_ROBIN = "RoundRobin"
    UNIFORM_WITH_REPLACEMENT = "uniform_with_replacement"
    SAMPLING = "sampling"


class Multiplexer(Composable):
    """Multiplexing over a list of composables.

    The challenge of multiplexing from a iterstream is the lack of knowledge of the
    length of each stream. Hence it is important to understand that the only option to
    control the sampling ratio is to fix the dataset distribution ratio *AT THE OUTPUT*
    of the iterator, i.e. the ratio of datapoints after collection the samples of the
    composable. In other words: If T_{eff} is the token number of training dataset then

        w_{i, eff} = t_{i, eff} / T_{eff}

    is the effective ratio of dataset `i` and t_{i, eff} the number of tokens it
    contributes to the final output dataset. Defining the number of tokens in dataset
    `i` as t_i, then the oversampling ratio is defined as

        e_i = t_{i, eff} / t_i.

    The quantity e_i is also the number of epochs that dataset `i` is being iterated
    over during training on T_{eff} tokens.

    Hence we have two knobs we can control in the multiplexing algorithm:

        1. The sampling probabilities w_{i, eff}
        2. The maximum oversampling epochs e_i

    Note that these two quanitities are not independent of each other and fixing both
    conditions at the same time does not guarantee that both are fulfilled at the same
    time.
    """

    def __init__(
        self,
        composables: t.List[Composable],
        mux_strategy: MultiplexingStrategy,
        sampling_probas: t.Optional[t.List[float]] = None,
        max_reinits: t.Optional[int] = None,
        seed: t.Optional[int] = None,
        proba_threshold: float = 1e-3,
        **kwargs,
    ) -> None:
        """Initializes a multiplexer of a list of data iterstream composables.

        Args:
            composables: List of composables corresponding to different data sources.
                Note: Ensure that the composable are properly split according to
                multiprocessing worker and / or GPU ranks. There are several ways to
                do this, and we encourage reading the documentation for the corresponding
                driver or the composable you are using. As an example you can load a
                message pack drive using:
                ```python

                c0 = MessagepackDriver(url=local_msgpack_url).get_iter(key_hooks=[SplitByWorker])
                c1 = MessagepackDriver(url=local_msgpack_url).get_iter(key_hooks=[SplitByWorker])

                mux = Multiplexer([c0, c1]).to_torch_iterable(False, False)
                ```
            mux_strategy: Multiplexing strategy.
            sampling_probas: (optional) list of floats that determine the sampling
                ratios for each dataset, i.e. w_{i, eff} from the docstrings.
            max_reinits: (optional) int that determines the maximum oversampling
                epochs over all datasets.
            seed: (optional) int for the random number generator.
            proba_threshold: float to indicate when a composable should be ignored
                in sampling.

        Note that the algorithm stops whenever max_reinits are hit or all composables
        have been reinitialized at least once.
        """
        super().__init__(source=composables)
        self.mux_strategy = mux_strategy
        if mux_strategy == MultiplexingStrategy.SAMPLING:
            assert sampling_probas is not None
            assert len(sampling_probas) == len(
                self.source
            ), "Need sampling probas and composables to have same number of entries"

        self.composables, self.sampling_probas = self._init_composables_and_probas(
            sampling_probas, composables, proba_threshold
        )
        self.rng = np.random.RandomState(np.random.MT19937(seed=seed))
        self._reinit_counts_ = []
        self._num_samples_ = len(self.composables) * [0]
        self.max_reinits = sys.maxsize
        if max_reinits is not None:
            self.max_reinits = max_reinits

    def _init_composables_and_probas(
        self,
        probas: t.List[float],
        composables: t.List[Composable],
        proba_threshold: float = 1e-3,
    ) -> t.Tuple[t.List[Composable], t.Optional[t.List[float]]]:
        """Remove composables with too small probabilities.

        In order to prevent problems with extremely tiny sampling ratios we assume
        that any ratio beloe `proba_threshold` is de-facto zero and we ignore the
        corresponding composables

        Args:
            probas: list of float determining the sampling ratios.
            composables: list of composables corresponding to different data sources.
            proba_threshold: float to indicate when a composable should be ignored
                in sampling.
        """
        if probas is None or self.mux_strategy == MultiplexingStrategy.ROUND_ROBIN:
            return composables, None
        else:
            _p = np.asarray(probas)
            idx = np.where(_p > proba_threshold)[0]

            # need to renormalize probabilities after removal
            # to prevent rng.sampling from barfing
            _p = _p[idx]
            _p /= _p.sum()
            return np.asarray(composables)[idx].tolist(), _p.tolist()

    def _get_iterator_sequence(self, num_iterators: int) -> t.Generator:
        """Defines the random sequence control for the iterators.

        Returns a generator of random numbers over the composables indices to
        indicate which composable to sample from next according to the multiplexing
        strategy.

        Args:
            num_iterators: int of the number of sampling composables.
        """
        if num_iterators == 0:
            return
        if self.mux_strategy == MultiplexingStrategy.ROUND_ROBIN:
            yield from itertools.cycle(range(num_iterators))
        if self.mux_strategy == MultiplexingStrategy.UNIFORM_WITH_REPLACEMENT:
            while True:
                yield self.rng.choice(num_iterators, 1, replace=True).squeeze()
        if self.mux_strategy == MultiplexingStrategy.SAMPLING:
            while True:
                yield self.rng.choice(num_iterators, 1, replace=True, p=self.sampling_probas).squeeze()
        else:
            raise NotImplementedError(f"Multiplexing strategy {self.mux_strategy} not implemented.")

    @property
    def reinit_counts(self) -> t.List[int]:
        """Return number of reinits for non-zero probability composables."""
        return self._reinit_counts_

    @property
    def num_samples(self) -> t.List[int]:
        """Return number of samples seen from each composable."""
        return self._num_samples_

    def _compute_reinit_counts(self, _iterators: t.List[MuxIterator]) -> t.List[int]:
        """Compute the reinit counts over a list of MuxIterators.

        Args:
            _iterators: list of MuxIterators whose reinit counts are to be aggregated.
        """
        self._reinit_counts_ = [mux.reinits for mux in _iterators]
        return self._reinit_counts_

    def _iteration_done(self, _iterators: t.List[MuxIterator]) -> bool:
        """True if every iterator has been iterated over once.

        This ensures that we can oversample all datasets but stop when we iterated
        the longest iterator once. Alternatively it stops when a dataset reached its
        maximum oversampling count.

        Args:
            _iterators: list of MuxIterators whose reinit counts are to be aggregated.
        """
        return (
            min(self._compute_reinit_counts(_iterators)) > 0
            or max(self._compute_reinit_counts(_iterators)) == self.max_reinits
        )

    def __iter__(self) -> t.Iterator[t.Dict[str, str]]:
        """Returns the multiplexed iterator."""
        # Instantiate _iterators in the local process
        # to avoid pickle errors when using multiprocessing
        # to fork/spawn multiple dataloader workers.
        _iterators = [MuxIterator(idx, iter(c), reinits=0) for idx, c in enumerate(self.composables)]
        index_generator = self._get_iterator_sequence(len(_iterators))
        while True:
            _ix = next(index_generator, None)
            if _ix is None:
                logger.info("No more samples for multiplexing. Done with all composable" " iterators.")
                return
            logger.debug(f"Sampling from iterator {_ix}")
            mux_it: MuxIterator = _iterators[_ix]
            try:
                value = next(mux_it.it)
                self._num_samples_[mux_it.index] += 1
                yield value
            except StopIteration:
                logger.info(
                    f"Composable iterator {mux_it.index} ran out of samples." " Handling termination condition now."
                )
                if self.mux_strategy == MultiplexingStrategy.ROUND_ROBIN:
                    logger.info(
                        f"Removing iterator {mux_it.index} from sampling as" " part of the round robin strategy."
                    )
                    _ = _iterators.remove(mux_it)
                    index_generator = self._get_iterator_sequence(len(_iterators))
                else:
                    _iterators[mux_it.index] = MuxIterator(
                        mux_it.index,
                        iter(self.composables[mux_it.index]),
                        mux_it.reinits + 1,
                    )
                    logger.info(
                        f"Reinitializing iterator {mux_it.index} as part of the"
                        f" {self.mux_strategy} sampling strategy. Now at reinit"
                        f" count {mux_it.reinits}."
                    )
                    if self._iteration_done(_iterators):
                        logger.info("Multiplexing termination condition reached.")
                        return
