import pickle
import typing as t

import numpy as np


def getsize(item: t.Any) -> int:
    """Return estimated size (in terms of bytes) of a python object. Currently use numpy method to calculate size.
    Otherwise, the size is estimated through its pickled size. This is considered a better performant option than
    `zarr.storage.getsize()`.
    """
    if isinstance(item, np.ndarray):
        size = item.nbytes
    else:
        size = len(pickle.dumps(item))
    return size
