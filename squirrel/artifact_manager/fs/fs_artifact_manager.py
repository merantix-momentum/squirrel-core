import typing as t
import os
from pathlib import Path
from typing import Optional, Any, Union, List, Iterable

from squirrel.artifact_manager.base import ArtifactManager
from squirrel.catalog import Catalog, Source
from squirrel.serialization import MessagepackSerializer, JsonSerializer, SquirrelSerializer
from squirrel.store import FilesystemStore
from squirrel.store.filesystem import get_random_key

Serializers = {
    MessagepackSerializer: "messagepack",
    JsonSerializer: "jsonl",
}


class ArtifactFileStore(FilesystemStore):
    def complete_key(self, key: str, **open_kwargs) -> List[str]:
        """Returns a list of possible key continuations given a partial key."""
        if not self.fs.exists(f"{self.url}/{key}", **open_kwargs):
            return []
        return [os.path.relpath(path, f"{self.url}/{key}") for path in self.fs.ls(f"{self.url}/{key}", detail=False, **open_kwargs)]

    def key_exists(self, key: str, **open_kwargs) -> bool:
        """Checks if a key exists."""
        return self.fs.exists(f"{self.url}/{key}", **open_kwargs)

    def get(self, key: str, mode: str = "rb", **open_kwargs) -> t.Any:
        """Retrieves an item with the given key."""
        if self.fs.exists(f"{self.url}/{key}/{Serializers[self.serializer.__class__]}", **open_kwargs):
            return super(ArtifactFileStore, self).get(f"{key}/{Serializers[self.serializer.__class__]}", mode,
                                                      **open_kwargs)
        elif self.fs.exists(f"{self.url}/{key}/file", **open_kwargs):
            return self.fs.cat(f"{self.url}/{key}/file", **open_kwargs)
        else:
            raise ValueError(f"Key {key} does not exist!")

    def set(self, value: t.Any, key: t.Optional[str] = None, mode: str = "wb", **open_kwargs) -> None:
        """Persists an item with the given key."""
        if key is None:
            key = get_random_key()
        if isinstance(value, Path):
            self.fs.cp(value, f"{self.url}/{key}/file", **open_kwargs)
        else:
            super(ArtifactFileStore, self).set(value, f"{key}/{Serializers[self.serializer.__class__]}", mode,
                                               **open_kwargs)


class FileSystemArtifactManager(ArtifactManager):
    def __init__(self, url: str, serializer: Optional[SquirrelSerializer] = MessagepackSerializer(), **fs_kwargs):
        super().__init__()
        self.backend = ArtifactFileStore(url=url, serializer=serializer, **fs_kwargs)

    def list_collection_names(self) -> Iterable:
        return self.backend.keys(nested=False)

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None) -> Any:
        """Retrieve specific artifact value."""
        if collection is None:
            collection = self.collection
        if version is None:
            version = len(self.backend.complete_key(os.path.join(collection, artifact)))
        if not self.backend.key_exists(os.path.join(collection, artifact, str(version))):
            raise ValueError(f"Artifact {artifact} does not exist in collection {collection} with version {version}!")
        path = os.path.join(collection, artifact, str(version))
        return self.backend.get(path)

    def get_artifact_source(
            self,
            artifact: str,
            collection: Optional[str] = None,
            version: Optional[int] = None
    ) -> Source:
        """Catalog entry for a specific artifact"""
        if collection is None:
            collection = self.collection
        if version is None:
            version = len(self.backend.complete_key(os.path.join(collection, artifact)))
        if not self.backend.key_exists(os.path.join(collection, artifact, str(version))):
            raise ValueError(f"Artifact {artifact} does not exist in collection {collection} with version {version}!")

        if (encoding := Serializers[self.backend.serializer.__class__]) in self.backend.complete_key(os.path.join(collection, artifact, str(version))):
            return Source(
                driver_name=encoding,
                driver_kwargs={
                    "url": os.path.join(self.backend.url, collection, artifact, str(version), encoding),
                    "storage_options": self.backend.storage_options
                },
                metadata={
                    "collection": collection,
                    "artifact": artifact,
                    "version": version
                }
            )
        elif self.backend.fs.exists(os.path.join(collection, artifact, str(version), "file")):
            return Source(
                driver_name="file",
                driver_kwargs={
                    "url": os.path.join(self.backend.url, collection, artifact, str(version), "file"),
                    "storage_options": self.backend.storage_options
                },
                metadata={
                    "collection": collection,
                    "artifact": artifact,
                    "version": version
                }
            )
        else:
            raise ValueError(f"Could not read artifact {artifact} in collection {collection} with version {version}!")

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Provide catalog of all artifacts and their versions contained within specific collection"""
        if collection is None:
            collection = self.collection
        catalog = Catalog()
        for artifact in self.backend.complete_key(collection):
            for version in self.backend.complete_key(os.path.join(collection, artifact)):
                catalog[os.path.join(collection, artifact)] = self.get_artifact_source(artifact, collection, int(version))
        return catalog

    def store_to_catalog(self) -> Catalog:
        """Provide Catalog of all artifacts stored in backend"""
        catalog = Catalog()
        for collection in self.list_collection_names():
            catalog.update(self.collection_to_catalog(collection))
        return catalog

    def get_artifact_location(self, artifact: str, collection: Optional[str] = None,
                              version: Optional[int] = None) -> str:
        """Get full qualified path or wandb directory of artifact"""
        return self.get_artifact_source(artifact, collection, version).driver_kwargs["url"]

    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        """Upload local file to artifact store without serialisation"""
        if collection is None:
            collection = self.collection
        version = len(self.backend.complete_key(os.path.join(collection, name))) + 1
        self.backend.set(local_path, os.path.join(collection, name, str(version)))
        return self.get_artifact_source(name, collection)

    def log_collection(self, files: Union[Path, List[Path]], collection_name: Optional[str] = None) -> Catalog:
        """Log folder or set of local paths as collection of artifacts into store"""
        if isinstance(files, Path) and files.is_dir():
            if collection_name is None:
                collection_name = files
            files = list(files.iterdir())
        elif isinstance(files, Path):
            raise ValueError(f"Path {files} is not a directory!")
        else:
            if collection_name is None:
                raise ValueError("Collection name must be specified if files is not a directory!")
        for file in files:
            if file.is_file():
                self.log_file(file, file.name, collection_name)
        return self.collection_to_catalog(collection_name)

    def log_object(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log an arbitrary python object using store serialisation."""
        if collection is None:
            collection = self.collection
        if self.backend.key_exists(os.path.join(collection, name)):
            version = len(self.backend.complete_key(os.path.join(collection, name))) + 1
        else:
            version = 1
        self.backend.set(obj, os.path.join(collection, name, str(version)))
        return self.get_artifact_source(name, collection)

    def download_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None,
                          to: Path = "./") -> Source:
        location = self.get_artifact_location(artifact, collection, version)
        if self.backend.fs.exists(location):
            self.backend.fs.cp(location, to)
            return Source(
                driver_name="file",
                driver_kwargs={
                    "url": str(to),
                    "storage_options": self.backend.storage_options
                },
                metadata={
                    "collection": collection,
                    "artifact": artifact,
                    "version": version
                }
            )

    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        catalog = self.collection_to_catalog(collection)
        for artifact in catalog.values():
            artifact_name = artifact.metadata["artifact"]
            self.download_artifact(artifact_name, to=to / artifact_name)
        return catalog
