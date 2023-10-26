import tempfile

from squirrel.artifact_manager.fs.fs import FileSystemArtifactManager


def test_log_object() -> None:
    """Log an object to the default collection and check details of the catalog entry."""
    obj = {"name": "John", "age": 30}
    artifact_name = "john"
    collection = "my_collection"

    tmpdir = tempfile.TemporaryDirectory()
    manager = FileSystemArtifactManager(url=tmpdir.name, auto_mkdir=True)
    source = manager.log_object(obj, artifact_name)

    assert source.driver_name == "messagepack"
    assert source.driver_kwargs["url"] == f"{tmpdir.name}/default/{artifact_name}/1/messagepack"
    assert source.metadata["collection"] == "default"
    assert source.metadata["artifact"] == artifact_name
    assert source.metadata["version"] == 1

    obj2 = {"name": "John", "age": 15}
    artifact_name = "young_john"

    source2 = manager.log_object(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"{tmpdir.name}/{collection}/{artifact_name}/1/messagepack"
    assert source2.metadata["collection"] == collection
    assert source2.metadata["artifact"] == artifact_name
    assert source2.metadata["version"] == 1

    source2 = manager.log_object(obj2, artifact_name, collection)
    assert source2.driver_kwargs["url"] == f"{tmpdir.name}/{collection}/{artifact_name}/2/messagepack"
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
    manager.log_object(obj, artifact_name, collection)
    assert manager.get_artifact("john", collection) == obj
    manager.log_object(obj1, artifact_name, collection)
    assert manager.get_artifact("john", collection) == obj1
    manager.log_object(obj2, artifact_name, collection)
    assert manager.get_artifact(artifact_name, collection) == obj2
    assert manager.get_artifact(artifact_name, collection, 2) == obj1
    assert manager.get_artifact(artifact_name, collection, 1) == obj
