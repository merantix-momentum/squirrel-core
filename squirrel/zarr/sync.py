from threading import Lock

from zarr.sync import ThreadSynchronizer


class SquirrelThreadSynchronizer(ThreadSynchronizer):
    """Provides synchronization using thread locks."""

    def __init__(self):
        """Modify ThreadSynchronizer, periodically remove unlocked locks to avoid excessive memory consumption."""
        super().__init__()
        self.max_len = 1000

    def __getitem__(self, item: str) -> Lock:
        """Return a lock from cache if exists, otherwise create a new lock."""
        if len(self.locks) >= self.max_len:
            self._remove_unlocked_locks()
        with self.mutex:
            return self.locks[item]

    def _remove_unlocked_locks(self) -> None:
        to_remove = [k for k, v in self.locks.items() if not v.locked()]
        for k in to_remove:
            self.locks.pop(k, None)
