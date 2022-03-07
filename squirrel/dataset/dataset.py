import abc

from squirrel.dataset.stream_dataset import Fetcher, StreamDataset


class AbstractDataset(abc.ABC):
    @staticmethod
    def get_stream(fetcher: Fetcher, cache_size: int, max_workers: int) -> StreamDataset:
        """Returns a :py:class:`~squirrel.dataset.stream_dataset.StreamDataset` that can stream items from/to the
        dataset.

        Args:
            fetcher (Fetcher): Fetcher instance to be used for reading/writing items.
            cache_size (int): Number of items to buffer. This should be set based on how big the items are. Should
                be greater than `max_workers`.
            max_workers (int): Maximum number of workers in the process pool.

        Returns:
            StreamDataset: Dataset that can stream items from/to the dataset.
        """
        pass
