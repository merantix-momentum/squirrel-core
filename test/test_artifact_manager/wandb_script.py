"""
Test script for the wandb artifact manager implementation. This is merely a substitute for a proper test suite until we
can figure out how to best mock the wandb API.
"""
from pathlib import Path

import tempfile
import wandb
from squirrel.artifact_manager.wandb import WandbArtifactManager

src_dir = tempfile.TemporaryDirectory()

test_run = wandb.init(project="sebastian-sandbox")
artifact_manager = WandbArtifactManager(project="sebastian-sandbox")
collection = "test_objects"

file_descriptions = [
    ("foo.txt", "foo", "v0", "Test: Foo"),
    ("bar.txt", "bar", "v0", "Test: Bar"),
    ("baz.txt", "bar", "v1", "Test: Baz"),
]

for (filename, artifact_name, version, content) in file_descriptions:
    with open(f"{src_dir.name}/{filename}", "w") as f:
        f.write(content)

    source = artifact_manager.log_files(artifact_name, Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    assert source.driver_name == "wandb"
    assert source.metadata["collection"] == collection, f"Value {source.metadata['collection']} should be {collection}"
    assert (
        source.metadata["artifact"] == artifact_name
    ), f"Value {source.metadata['artifact']} should be {artifact_name}"
    assert source.metadata["version"] == version, f"Value {source.metadata['version']} should be {version}"

# test logging of folder
source = artifact_manager.log_files("folder", Path(src_dir.name), collection)

assert source.driver_name == "wandb"
assert source.metadata["collection"] == collection, f"Value {source.metadata['collection']} should be {collection}"
assert source.metadata["artifact"] == "folder", f"Value {source.metadata['artifact']} should be {artifact_name}"
assert source.metadata["version"] == "v0", f"Value {source.metadata['version']} should be {version}"


# Test retrieval of specific files
for (filename, artifact_name, version, content) in file_descriptions:
    artifact_manager.download_artifact(artifact_name, collection, version, Path(f"{src_dir.name}/downloaded"))
    with open(f"{src_dir.name}/downloaded/{filename}") as f:
        assert f.read() == content

# Test retrieval of entire folder
artifact_manager.download_artifact("folder", collection, "v0", Path(f"{src_dir.name}/downloaded2"))
for (filename, _, _, content) in file_descriptions:
    with open(f"{src_dir.name}/downloaded2/{filename}") as f:
        assert f.read() == content

# test artifact existence checks
assert artifact_manager.exists("foo")
assert artifact_manager.exists("bar")

assert artifact_manager.exists_in_collection("foo", collection)
assert artifact_manager.exists_in_collection("bar", collection)
assert not artifact_manager.exists_in_collection("foo", "other_collection")

# test catalog retrieval
collection_catalog = artifact_manager.collection_to_catalog(collection)
assert f"{collection}/foo" in collection_catalog
assert f"{collection}/bar" in collection_catalog

catalog = artifact_manager.store_to_catalog()
assert f"{collection}/foo" in catalog
assert f"{collection}/bar" in catalog

# test log folder
with artifact_manager.log_folder("test_folder", collection) as folder:
    for (filename, _, _, content) in file_descriptions:
        with open(f"{folder}/{filename}", "w") as file:
            file.write(content)

assert artifact_manager.exists_in_collection("test_folder", collection)
source = artifact_manager.collection_to_catalog(collection)[f"{collection}/test_folder"]
assert source.driver_name == "wandb"
assert source.metadata["collection"] == collection
assert source.metadata["artifact"] == "test_folder"
assert source.metadata["version"] == "v0"

local_dir = tempfile.TemporaryDirectory()
artifact_manager.download_artifact("test_folder", collection, "v0", Path(f"{local_dir.name}/downloaded3"))
for (filename, _, _, content) in file_descriptions:
    with open(f"{local_dir.name}/downloaded3/{filename}") as f:
        assert f.read() == content

# test download collection
artifact_manager.download_collection(collection, Path(src_dir.name, "my_collection"))
for (filename, _, _, content) in file_descriptions:
    with open(f"{src_dir.name}/my_collection/test_folder/{filename}") as f:
        assert f.read() == content
with open(f"{src_dir.name}/my_collection/foo/foo.txt") as f:
    assert f.read() == "Test: Foo"
with open(f"{src_dir.name}/my_collection/bar/baz.txt") as f:
    assert f.read() == "Test: Baz"

wandb.finish()
src_dir.cleanup()
