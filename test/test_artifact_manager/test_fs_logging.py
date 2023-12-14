import tempfile
from pathlib import Path

import pytest

from squirrel.artifact_manager.fs import FileSystemArtifactManager
from squirrel.catalog import Source
from squirrel.serialization import JsonSerializer, MessagepackSerializer, SquirrelSerializer


@pytest.mark.parametrize(
    "serializer,name,target",
    [
        (JsonSerializer(), "jsonl", b'{"name": "John", "age": 30}'),
        (MessagepackSerializer(), "messagepack", b"\x82\xa4name\xa4John\xa3age\x1e"),
    ],
)
def test_serializer(serializer: SquirrelSerializer, name: str, target: str) -> None:
    """Test that artifacts are correctly serialised."""
    obj = {"name": "John", "age": 30}
    artifact_name = "john"

    tmpdir = tempfile.TemporaryDirectory()
    # message pack serialization
    manager = FileSystemArtifactManager(url=tmpdir.name, serializer=serializer, auto_mkdir=True)
    source = manager.log_artifact(obj, artifact_name)

    assert source.metadata["location"] == f"file://{tmpdir.name}/default/{artifact_name}/1/{name}"
    with open(f"{tmpdir.name}/default/{artifact_name}/1/{name}", "rb") as f:
        assert f.read() == target

    tmpdir.cleanup()


def test_multi_serializer() -> None:
    """Test that multiple artifact stores with differing backends are correctly interacting."""
    obj = {"name": "John", "age": 30}
    artifact_name = "john"

    tmpdir = tempfile.TemporaryDirectory()
    # message pack serialization
    msgpack_manager = FileSystemArtifactManager(url=tmpdir.name, serializer=MessagepackSerializer(), auto_mkdir=True)
    msgpack_source = msgpack_manager.log_artifact(obj, artifact_name)
    jsonl_manager = FileSystemArtifactManager(url=tmpdir.name, serializer=JsonSerializer(), auto_mkdir=True)
    jsonl_source = jsonl_manager.log_artifact(obj, artifact_name)

    assert msgpack_source == Source(
        driver_name="messagepack",
        driver_kwargs={
            "url": f"file://{tmpdir.name}/default/john/1/messagepack",
            "storage_options": {"auto_mkdir": True},
        },
        metadata={
            "collection": "default",
            "artifact": "john",
            "version": 1,
            "location": f"file://{tmpdir.name}/default/john/1/messagepack",
        },
    )
    assert jsonl_source == Source(
        driver_name="jsonl",
        driver_kwargs={"url": f"file://{tmpdir.name}/default/john/2/jsonl", "storage_options": {"auto_mkdir": True}},
        metadata={
            "collection": "default",
            "artifact": "john",
            "version": 2,
            "location": f"file://{tmpdir.name}/default/john/2/jsonl",
        },
    )
    tmpdir.cleanup()


def test_log_object() -> None:
    """Log an object to the default collection and check details of the catalog entry."""
    obj = {"name": "John", "age": 30}
    artifact_name = "john"
    collection = "my_collection"

    tmpdir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=tmpdir.name, auto_mkdir=True)
    source = manager.log_artifact(obj, artifact_name)

    assert source.driver_name == "messagepack"
    assert source.driver_kwargs["url"] == f"file://{tmpdir.name}/default/{artifact_name}/1/messagepack"
    assert source.metadata["collection"] == "default"
    assert source.metadata["artifact"] == artifact_name
    assert source.metadata["version"] == 1

    obj2 = {"name": "John", "age": 15}
    artifact_name = "young_john"

    source2 = manager.log_artifact(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"file://{tmpdir.name}/{collection}/{artifact_name}/1/messagepack"
    assert source2.metadata["collection"] == collection
    assert source2.metadata["artifact"] == artifact_name
    assert source2.metadata["version"] == 1

    source2 = manager.log_artifact(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"file://{tmpdir.name}/{collection}/{artifact_name}/2/messagepack"
    assert source2.metadata["collection"] == collection
    assert source2.metadata["artifact"] == artifact_name
    assert source2.metadata["version"] == 2
    tmpdir.cleanup()


def test_get_artifact() -> None:
    """Test logging multiple versions of the same object and retrieving different versions."""
    obj = {"name": "John", "age": 10}
    obj1 = {"name": "John", "age": 20}
    obj2 = {"name": "John", "age": 30}
    artifact_name = "john"
    collection = "my_collection"

    tmpdir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=tmpdir.name, auto_mkdir=True)
    manager.log_artifact(obj, artifact_name, collection)
    assert manager.get_artifact("john", collection) == obj
    manager.log_artifact(obj1, artifact_name, collection)
    assert manager.get_artifact("john", collection) == obj1
    manager.log_artifact(obj2, artifact_name, collection)
    assert manager.get_artifact(artifact_name, collection) == obj2
    assert manager.get_artifact(artifact_name, collection, 2) == obj1
    assert manager.get_artifact(artifact_name, collection, 1) == obj


def test_log_file() -> None:
    """Test logging files to the artifact store."""
    src_dir = tempfile.TemporaryDirectory()
    collection = "my_collection"

    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    for (filename, artifact_name, version, content) in [
        ("foo.txt", "foo_file", 1, "Test: Foo"),
        ("bar.txt", "bar_file", 1, "Test: Bar"),
        ("baz.txt", "bar_file", 2, "Test: Baz"),
    ]:
        with open(f"{src_dir.name}/{filename}", "w") as f:
            f.write(content)

        source = manager.log_file(Path(f"{src_dir.name}/{filename}"), artifact_name, collection)

        assert source.driver_name == "file"
        assert source.metadata["collection"] == collection
        assert source.metadata["artifact"] == artifact_name
        assert source.metadata["version"] == version

        with open(f"{store_dir.name}/{collection}/{artifact_name}/{version}/file") as f:
            assert f.read() == content
    src_dir.cleanup()
    store_dir.cleanup()


def test_get_file() -> None:
    """Test retrieval of logged files from the artifact store."""
    src_dir = tempfile.TemporaryDirectory()
    collection = "my_collection"

    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    file_descriptions = [
        ("bar.txt", "bar_file", 1, "Test: Bar"),
        ("baz.txt", "bar_file", 2, "Test: Baz"),
    ]

    for (filename, artifact_name, _, content) in file_descriptions:
        with open(f"{src_dir.name}/{filename}", "w") as f:
            f.write(content)

        manager.log_file(Path(f"{src_dir.name}/{filename}"), artifact_name, collection)

    for (filename, artifact_name, version, content) in file_descriptions:
        manager.download_artifact(artifact_name, collection, version, Path(f"{src_dir.name}/downloaded_{filename}"))
        with open(f"{src_dir.name}/downloaded_{filename}") as f:
            assert f.read() == content

    src_dir.cleanup()
    store_dir.cleanup()
