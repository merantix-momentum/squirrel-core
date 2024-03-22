import os
import tempfile
from pathlib import Path

import pytest

from squirrel.artifact_manager.filesystem import FileSystemArtifactManager
from squirrel.catalog import Source
from squirrel.serialization import JsonSerializer, MessagepackSerializer, SquirrelSerializer


@pytest.mark.skip(reason="Logging of python values not yet supported")
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

    assert source.metadata["location"] == f"file://{tmpdir.name}/default/{artifact_name}/v0/{name}/john"
    with open(f"{tmpdir.name}/default/{artifact_name}/v0/{name}", "rb") as f:
        assert f.read() == target

    tmpdir.cleanup()


@pytest.mark.skip(reason="Logging of python values not yet supported")
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
            "url": f"file://{tmpdir.name}/default/john/v0/messagepack",
            "storage_options": {"auto_mkdir": True},
        },
        metadata={
            "collection": "default",
            "artifact": "john",
            "version": "v0",
            "location": f"file://{tmpdir.name}/default/john/v0/messagepack",
        },
    )
    assert jsonl_source == Source(
        driver_name="jsonl",
        driver_kwargs={"url": f"file://{tmpdir.name}/default/john/v1/jsonl", "storage_options": {"auto_mkdir": True}},
        metadata={
            "collection": "default",
            "artifact": "john",
            "version": "v1",
            "location": f"file://{tmpdir.name}/default/john/v1/jsonl",
        },
    )
    tmpdir.cleanup()


@pytest.mark.skip(reason="Logging of python values not yet supported")
def test_log_object() -> None:
    """Log an object to the default collection and check details of the catalog entry."""
    obj = {"name": "John", "age": 30}
    artifact_name = "john"
    collection = "my_collection"

    tmpdir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=tmpdir.name, auto_mkdir=True)
    source = manager.log_artifact(obj, artifact_name)

    assert source.driver_name == "messagepack"
    assert source.driver_kwargs["url"] == f"file://{tmpdir.name}/default/{artifact_name}/v0/messagepack"
    assert source.metadata["collection"] == "default"
    assert source.metadata["artifact"] == artifact_name
    assert source.metadata["version"] == "v0"

    obj2 = {"name": "John", "age": 15}
    artifact_name = "young_john"

    source2 = manager.log_artifact(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"file://{tmpdir.name}/{collection}/{artifact_name}/v0/messagepack"
    assert source2.metadata["collection"] == collection
    assert source2.metadata["artifact"] == artifact_name
    assert source2.metadata["version"] == "v0"

    source2 = manager.log_artifact(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"file://{tmpdir.name}/{collection}/{artifact_name}/v1/messagepack"
    assert source2.metadata["collection"] == collection
    assert source2.metadata["artifact"] == artifact_name
    assert source2.metadata["version"] == "v1"
    tmpdir.cleanup()


@pytest.mark.skip(reason="Logging of python values not yet supported")
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
    assert manager.get_artifact(artifact_name, collection, "v1") == obj1
    assert manager.get_artifact(artifact_name, collection, "v0") == obj


def test_log_file() -> None:
    """Test logging files to the artifact store."""
    src_dir = tempfile.TemporaryDirectory()
    collection = "my_collection"

    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    # test logging of individual files
    for (filename, artifact_name, version, content) in [
        ("foo.txt", "foo_file", 0, "Test: Foo"),
        ("bar.txt", "bar_file", 0, "Test: Bar"),
        ("baz.txt", "bar_file", 1, "Test: Baz"),
    ]:
        with open(f"{src_dir.name}/{filename}", "w") as f:
            f.write(content)

        source = manager.log_files(artifact_name, Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

        assert source.driver_name == "directory"
        assert source.metadata["collection"] == collection
        assert source.metadata["artifact"] == artifact_name
        assert source.metadata["version"] == f"v{version}"
        assert source.version == version + 1

        with open(f"{store_dir.name}/{collection}/{artifact_name}/v{version}/files/{filename}") as f:
            assert f.read() == content

    # test logging of folder
    source = manager.log_files("folder", Path(src_dir.name), collection)

    assert source.driver_name == "directory"
    assert source.metadata["collection"] == collection
    assert source.metadata["artifact"] == "folder"
    assert source.version == 1

    assert len(list(Path(f"{store_dir.name}/{collection}/folder/v0/files").iterdir())) == 3
    assert len([x for x in source.get_driver().keys()]) == 3
    src_dir.cleanup()
    store_dir.cleanup()


def test_exists() -> None:
    """Test existence checks for artifacts in the artifact store."""
    test_files = {
        "my_collection": ["foo.txt", "bar.txt"],
        "default": ["baz.txt"],
    }

    src_dir = tempfile.TemporaryDirectory()
    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    for collection in test_files:
        for filename in test_files[collection]:
            with open(f"{src_dir.name}/{filename}", "w") as f:
                f.write("Test")

            manager.log_files(filename[:3], Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    assert manager.exists("foo")
    assert manager.exists("bar")
    assert manager.exists("baz")

    assert manager.exists_in_collection("foo", "my_collection")
    assert manager.exists_in_collection("bar", "my_collection")
    assert not manager.exists_in_collection("baz", "my_collection")

    assert not manager.exists_in_collection("foo", "default")
    assert not manager.exists_in_collection("bar", "default")
    assert manager.exists_in_collection("baz", "default")

    src_dir.cleanup()
    store_dir.cleanup()


def test_get_file() -> None:
    """Test retrieval of logged files from the artifact store."""
    src_dir = tempfile.TemporaryDirectory()
    collection = "my_collection"

    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    file_descriptions = [
        ("bar.txt", "bar_file", "v0", "Test: Bar"),
        ("baz.txt", "bar_file", "v1", "Test: Baz"),
    ]

    for (filename, artifact_name, _, content) in file_descriptions:
        with open(f"{src_dir.name}/{filename}", "w") as f:
            f.write(content)

        manager.log_files(artifact_name, Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    manager.log_files("folder", Path(src_dir.name), collection)

    # Test retrieval of specific files
    for (filename, artifact_name, version, content) in file_descriptions:
        manager.download_artifact(artifact_name, collection, version, Path(f"{src_dir.name}/downloaded"))
        with open(f"{src_dir.name}/downloaded/{artifact_name}/{filename}") as f:
            assert f.read() == content

    # Test retrieval of entire folder
    manager.download_artifact("folder", collection, "v0", Path(f"{src_dir.name}/downloaded2"))
    for (filename, _, _, content) in file_descriptions:
        with open(f"{src_dir.name}/downloaded2/folder/{filename}") as f:
            assert f.read() == content

    src_dir.cleanup()
    store_dir.cleanup()


def test_log_folder() -> None:
    """Test logging of folders using the context manager provided by the artifact store."""
    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)
    collection = "my_collection"
    test_files = [
        ("foo.txt", "Test: Foo"),
        ("bar.txt", "Test: Bar"),
        ("baz.txt", "Test: Baz"),
    ]
    with manager.log_folder("folder", collection) as folder:
        for (filename, content) in test_files:
            with open(f"{folder}/{filename}", "w") as file:
                file.write(content)

    assert manager.exists_in_collection("folder", collection)
    source = manager.collection_to_catalog(collection)["my_collection/folder"]
    assert source.driver_name == "directory"
    assert source.metadata["collection"] == collection
    assert source.metadata["artifact"] == "folder"
    assert source.metadata["version"] == "v0"
    assert source.version == 1
    assert len(list(Path(f"{store_dir.name}/{collection}/folder/v0/files").iterdir())) == 3

    local_dir = tempfile.TemporaryDirectory()
    manager.download_artifact("folder", collection, "v0", Path(f"{local_dir.name}/downloaded"))
    for (filename, content) in test_files:
        with open(f"{local_dir.name}/downloaded/folder/{filename}") as f:
            assert f.read() == content

    local_dir.cleanup()
    store_dir.cleanup()


def test_store_to_catalog() -> None:
    """Test retrieval of catalog description of artifacts stored in the artifact store."""
    test_files = {
        "my_collection": ["foo.txt", "bar.txt"],
        "default": ["baz.txt"],
    }

    src_dir = tempfile.TemporaryDirectory()
    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    for collection in test_files:
        for filename in test_files[collection]:
            with open(f"{src_dir.name}/{filename}", "w") as f:
                f.write("Test")

            manager.log_files(filename[:3], Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    collection_catalog = manager.collection_to_catalog("my_collection")
    assert len(collection_catalog) == 2
    assert "my_collection/foo" in collection_catalog
    assert "my_collection/bar" in collection_catalog

    default_catalog = manager.collection_to_catalog()
    assert len(default_catalog) == 1
    assert "default/baz" in default_catalog

    catalog = manager.store_to_catalog()
    assert len(catalog) == 3
    assert "my_collection/foo" in catalog
    assert catalog["my_collection/foo"].metadata["collection"] == "my_collection"
    assert catalog["my_collection/foo"].metadata["artifact"] == "foo"
    assert catalog["my_collection/foo"].version == 1
    assert "my_collection/bar" in catalog
    assert catalog["my_collection/bar"].metadata["collection"] == "my_collection"
    assert catalog["my_collection/bar"].metadata["artifact"] == "bar"
    assert catalog["my_collection/bar"].version == 1
    assert "default/baz" in catalog
    assert catalog["default/baz"].metadata["collection"] == "default"
    assert catalog["default/baz"].metadata["artifact"] == "baz"
    assert catalog["default/baz"].version == 1

    src_dir.cleanup()
    store_dir.cleanup()


def test_download_collection() -> None:
    """Test retrieval of an entire collection from the artifact store."""
    test_files = {
        "my_collection": ["foo.txt", "bar.txt"],
        "default": ["baz.txt"],
    }

    src_dir = tempfile.TemporaryDirectory()
    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)

    for collection in test_files:
        for filename in test_files[collection]:
            with open(f"{src_dir.name}/{filename}", "w") as f:
                f.write("Test")

            manager.log_files(filename[:3], Path(f"{src_dir.name}/{filename}"), collection, Path(filename))

    cat1 = manager.download_collection(Path(src_dir.name, "downloaded/my_collection"), "my_collection")
    assert len(cat1) == 2
    cat2 = manager.download_collection(Path(src_dir.name, "downloaded/default"), "default")
    assert len(cat2) == 1
    for collection in test_files:
        for filename in test_files[collection]:
            with open(f"{src_dir.name}/downloaded/{collection}/{filename[:3]}/{filename}") as f:
                assert f.read() == "Test"

    src_dir.cleanup()
    store_dir.cleanup()


def test_download_tmp() -> None:
    """Test temporary retrieval of artifacts."""
    store_dir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=store_dir.name, auto_mkdir=True)
    collection = "my_collection"
    test_files = {
        "foo.txt": "Test: Foo",
        "bar.txt": "Test: Bar",
        "baz.txt": "Test: Baz",
    }
    with manager.log_folder("folder", collection) as folder:
        for (filename, content) in test_files.items():
            with open(f"{folder}/{filename}", "w") as file:
                file.write(content)

    with manager.download_artifact("folder", collection, "v0") as path:
        for file in os.listdir(path):
            with open(f"{path}/{file}") as f:
                assert f.read() == test_files[file]
