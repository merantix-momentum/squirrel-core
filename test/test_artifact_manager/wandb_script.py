from pathlib import Path

import numpy as np
import pandas as pd
import tempfile
import wandb
from squirrel.artifact_manager.wandb import WandbArtifactManager


tbl_data = pd.DataFrame({"users": ["geoff", "juergen", "ada"], "feature_01": [1, 117, 42]})
tbl = wandb.Table(data=tbl_data)

pixels = np.random.randint(low=0, high=256, size=(100, 100, 3))
image = wandb.Image(pixels, caption=f"random field")

localdir = tempfile.TemporaryDirectory()
tbl_data.to_csv(f"{localdir.name}/tbl")
pixels.dump(f"{localdir.name}/image")

test_run = wandb.init(project="sebastian-sandbox")

artifact_manager = WandbArtifactManager(project="sebastian-sandbox")
# src1 = artifact_manager.log_artifact(tbl, "tbl", collection="test_objects")
# assert src1.metadata["collection"] == "test_objects"
# assert src1.metadata["artifact"] == "tbl"
# assert src1.metadata["version"] == "v0", f"First version should be v1, but is {src1.metadata['version']}"
# assert src1.metadata["location"] == Path("https://merantix-momentum.wandb.io/mxm/sebastian-sandbox/test_objects/tbl/v0"), \
#     f"Location is {src1.metadata['location']}"
#
# src2 = artifact_manager.log_artifact(image, "random_field", collection="test_data")
# assert src2.metadata["collection"] == "test_data"
# assert src2.metadata["artifact"] == "random_field", f"Name should be random_field, but is {src2.metadata['artifact']}"
# assert src2.metadata["version"] == "v0", f"First version should be v0, but is {src2.metadata['version']}"
# assert src2.metadata["location"] == Path("https://merantix-momentum.wandb.io/mxm/sebastian-sandbox/test_data/random_field/v0")
#
# catalog = artifact_manager.log_folder(Path(localdir.name), "test_objects")
# assert len(catalog) == 2, f"Catalog is {catalog}"
# assert "test_objects/tbl" in catalog and "test_objects/image" in catalog, f"Catalog is {catalog}"
# assert catalog["test_objects/tbl"].metadata["version"] == "v1", f"Catalog entry is {catalog['test_objects/tbl']}"
# assert catalog["test_objects/image"].metadata["version"] == "v0", f"Catalog entry is {catalog['test_objects/image']}"

assert artifact_manager.get_artifact("tbl", collection="test_objects", version="v0") == tbl, artifact_manager.get_artifact("tbl", collection="test_objects")
assert (
        np.array(artifact_manager.get_artifact("random_field", collection="test_data").image.getdata())
        .reshape((100,100,3)) == pixels),\
    artifact_manager.get_artifact("random_field", collection="test_data")

artifact_manager.download_collection(collection="test_objects", to=Path(localdir.name, "recovered"))

wandb.finish()
localdir.cleanup()
