from pathlib import Path

import tempfile
import wandb
from squirrel.artifact_manager.wandb import WandbArtifactManager

src_dir = tempfile.TemporaryDirectory()

test_run = wandb.init(project="sebastian-sandbox")
artifact_manager = WandbArtifactManager(project="sebastian-sandbox")
collection = "test_objects"

file_descriptions = [
    ("foo.txt", "foo_file", "v0", "Test: Foo"),
    ("bar.txt", "bar_file", "v0", "Test: Bar"),
    ("baz.txt", "bar_file", "v1", "Test: Baz"),
]

for (filename, artifact_name, version, content) in file_descriptions:
    with open(f"{src_dir.name}/{filename}", "w") as f:
        f.write(content)

    source = artifact_manager.log_files(artifact_name, Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    assert source.driver_name == "wandb"
    assert source.metadata["collection"] == collection, f"Value {source.metadata['collection']} should be {collection}"
    assert source.metadata["artifact"] == artifact_name, f"Value {source.metadata['artifact']} should be {artifact_name}"
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
for (filename, artifact_name, version, content) in file_descriptions:
    with open(f"{src_dir.name}/downloaded2/{filename}") as f:
        assert f.read() == content


wandb.finish()
src_dir.cleanup()
