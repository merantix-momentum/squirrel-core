import typing as t
from pathlib import Path
from typing import Optional, Any, List, Iterable

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
    def complete_key(self, partial_key: Path, **open_kwargs) -> List[str]:
        """Returns a list of possible key continuations given a partial key."""
        full_path = Path(self.url, partial_key)
        if not self.fs.exists(full_path, **open_kwargs):
            return []
        return [str(Path(path).relative_to(full_path)) for path in self.fs.ls(full_path, detail=False, **open_kwargs)]

    def key_exists(self, key: Path, **open_kwargs) -> bool:
        """Checks if a key exists."""
        return self.fs.exists(Path(self.url) / key, **open_kwargs)

    def get(self, key: Path, mode: str = "rb", **open_kwargs) -> t.Any:
        """Retrieves an item with the given key."""
        if self.fs.exists(Path(self.url, key, Serializers[self.serializer.__class__]), **open_kwargs):
            return super().get(str(Path(key, Serializers[self.serializer.__class__])), mode, **open_kwargs)
        elif self.fs.exists(Path(self.url, key, "file"), **open_kwargs):
            return self.fs.cat(Path(self.url, key, "file"), **open_kwargs)
        else:
            raise ValueError(f"Key {key} does not exist!")

    def set(self, value: t.Any, key: t.Optional[Path] = None, mode: str = "wb", **open_kwargs) -> None:
        """Persists an item with the given key."""
        if key is None:
            key = get_random_key()
        if isinstance(value, Path):
            self.fs.cp(value, Path(self.url, key, "file"), **open_kwargs)
        else:
            super().set(value, str(Path(key, Serializers[self.serializer.__class__])), mode, **open_kwargs)


class FileSystemArtifactManager(ArtifactManager):
    def __init__(self, url: str, serializer: Optional[SquirrelSerializer] = None, **fs_kwargs):
        """
        Artifactmanager backed by fsspec filesystems.

        The manager logs artifacts according to the following file structure:
        url/collection/artifact/version/serializer
            url: root directory of the artifact store
            collection: the name of the collection defaults to 'default'
            artifact: (human-readable) name of the artifact
            version: version number starting at 1 which is automatically incremented
            serializer: the name of the serializer used to store the artifact (e.g. file, messagepack)
        """
        super().__init__()
        if serializer is None:
            serializer = MessagepackSerializer()
        self.backend = ArtifactFileStore(url=url, serializer=serializer, **fs_kwargs)

    def list_collection_names(self) -> Iterable:
        """List all collections managed by this ArtifactManager."""
        return self.backend.keys(nested=False)

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None) -> Any:
        """Retrieve specific artifact value."""
        if collection is None:
            collection = self.collection
        if version is None:
            version = len(self.backend.complete_key(Path(collection) / Path(artifact)))
        if not self.backend.key_exists(Path(collection, artifact, str(version))):
            raise ValueError(f"Artifact {artifact} does not exist in collection {collection} with version {version}!")
        path = Path(collection, artifact, str(version))
        return self.backend.get(path)

    def get_artifact_source(
        self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None
    ) -> Source:
        """Catalog entry for a specific artifact"""
        if collection is None:
            collection = self.collection
        if version is None:
            version = len(self.backend.complete_key(Path(collection, artifact)))
        if not self.backend.key_exists(Path(collection, artifact, str(version))):
            raise ValueError(f"Artifact {artifact} does not exist in collection {collection} with version {version}!")

        if Serializers[self.backend.serializer.__class__] in self.backend.complete_key(
            Path(collection, artifact, str(version))
        ):
            return Source(
                driver_name=Serializers[self.backend.serializer.__class__],
                driver_kwargs={
                    "url": Path(
                        self.backend.url,
                        collection,
                        artifact,
                        str(version),
                        Serializers[self.backend.serializer.__class__],
                    ).as_uri(),
                    "storage_options": self.backend.storage_options,
                },
                metadata={"collection": collection, "artifact": artifact, "version": version},
            )
        elif self.backend.fs.exists(Path(collection, artifact, str(version), "file")):
            return Source(
                driver_name="file",
                driver_kwargs={
                    "url": Path(self.backend.url, collection, artifact, str(version), "file").as_uri(),
                    "storage_options": self.backend.storage_options,
                },
                metadata={"collection": collection, "artifact": artifact, "version": version},
            )
        else:
            raise ValueError(f"Could not read artifact {artifact} in collection {collection} with version {version}!")

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Provide catalog of all artifacts and their versions contained within specific collection"""
        if collection is None:
            collection = self.collection
        catalog = Catalog()
        for artifact in self.backend.complete_key(Path(collection)):
            for version in self.backend.complete_key(Path(collection, artifact)):
                catalog[str(Path(collection, artifact))] = self.get_artifact_source(artifact, collection, int(version))
        return catalog

    def store_to_catalog(self) -> Catalog:
        """Provide Catalog of all artifacts stored in backend"""
        catalog = Catalog()
        for collection in self.list_collection_names():
            catalog.update(self.collection_to_catalog(collection))
        return catalog

    def get_artifact_location(
        self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None
    ) -> str:
        """Get full qualified path or wandb directory of artifact"""
        return self.get_artifact_source(artifact, collection, version).driver_kwargs["url"]

    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        """Upload local file to artifact store without serialisation"""
        if collection is None:
            collection = self.collection
        version = len(self.backend.complete_key(Path(collection, name))) + 1
        self.backend.set(local_path, Path(collection, name, str(version)))
        return self.get_artifact_source(name, collection)

    def log_files(self, local_paths: List[Path], collection: Optional[str] = None) -> Catalog:
        """Upload a collection of file into a (current) collection"""
        if collection is None:
            collection = self.collection
        for local_path in local_paths:
            self.log_file(local_path, local_path.name, collection)
        return self.collection_to_catalog(collection)

    def log_folder(self, file: Path, collection: Optional[str] = None) -> Catalog:
        """Log folder as collection of artifacts into store"""
        if not file.is_dir():
            raise ValueError(f"Path {file} is not a directory!")

        if collection is None:
            collection = file.name

        return self.log_files([f for f in file.iterdir() if f.is_file()], collection)

    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log an arbitrary python object using store serialisation."""
        if collection is None:
            collection = self.collection
        if self.backend.key_exists(Path(collection, name)):
            version = len(self.backend.complete_key(Path(collection, name))) + 1
        else:
            version = 1
        self.backend.set(obj, Path(collection, name, str(version)))
        return self.get_artifact_source(name, collection)

    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None, to: Path = "./"
    ) -> Source:
        """Download artifact to local path."""
        location = self.get_artifact_location(artifact, collection, version)
        if self.backend.fs.exists(location):
            self.backend.fs.cp(location, to)
            return Source(
                driver_name="file",
                driver_kwargs={"url": str(to), "storage_options": self.backend.storage_options},
                metadata={"collection": collection, "artifact": artifact, "version": version},
            )

    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        """Download all artifacts in collection to local directory."""
        catalog = self.collection_to_catalog(collection)
        for artifact in catalog.values():
            artifact_name = artifact.metadata["artifact"]
            self.download_artifact(artifact_name, to=to / artifact_name)
        return catalog
