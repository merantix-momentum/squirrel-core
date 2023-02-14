import typing as t

import numpy as np
import torchvision.transforms.functional as F
from create_sq_ds_from_hf import create_sq_ds_from_hf, to_np_dict


def to_np_dict_imagenette(
    item: t.Dict[str, t.Any],
    output_size: int = 300,
    img_key: str = "image",
) -> t.Dict[str, np.ndarray]:
    """Converts imagenette images to a fixed, square size."""

    item = to_np_dict(item)

    img = F.to_tensor(item[img_key])  # converts from HWC to CHW
    img = F.resize(img, output_size)
    img = F.center_crop(img, output_size)
    img = img.permute((1, 2, 0))  # convert back from CHW to HWC

    # handle grayscale images in imagenette which cause trouble downstream
    if img.shape[2] == 1:
        img = img.repeat(1, 1, 3)  # repeat channel dimension 3 times

    item[img_key] = img.numpy()

    return item


if __name__ == "__main__":
    create_sq_ds_from_hf(
        "frgfm/imagenette",
        to_np_dict=to_np_dict_imagenette,
        shard_size=10000,
        subset="320px",
        hf_split="train",
    )
