import typing as t
from pathlib import Path

import numpy as np
from squirrel.driver.msgpack import MessagepackDriver
from squirrel_datasets_core.driver.huggingface import HuggingfaceDriver


def to_np_dict(item: t.Dict[str, t.Any]) -> t.Dict[str, np.ndarray]:
    """Converts values in a dict to numpy arrays."""
    return {k: np.asarray(v) for k, v in item.items()}


def create_sq_ds_from_hf(
    hf_ds: str,
    hf_split: str,
    to_np_dict: t.Callable[[t.Any], t.Dict[str, np.ndarray]],
    shard_size: int,
    squirrel_url: str = None,
    **hf_kwargs,
) -> None:

    it = HuggingfaceDriver(hf_ds, **hf_kwargs).get_iter(hf_split)

    # create a default path from hf data
    if squirrel_url is None:
        url = Path(hf_ds) / Path(hf_split)
        url.mkdir(parents=True, exist_ok=True)
        url = str(url)  # squirrel wants str

    store = MessagepackDriver(url).store

    print("creating squirrel dataset under", url, "...")
    (
        it.tqdm()  #  prints "12345it [00:08, 5810.89it/s]"
        .map(to_np_dict)
        .batched(shard_size, drop_last_if_not_full=False)
        .map(store.set)
        .join()  # ensures direct iteration over stream
    )


if __name__ == "__main__":
    create_sq_ds_from_hf("cifar100", to_np_dict=to_np_dict, shard_size=10000, hf_split="train")
