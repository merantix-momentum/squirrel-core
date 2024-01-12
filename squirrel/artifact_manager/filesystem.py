from pathlib import Path
from typing import Any, Iterable, List, Optional, Union

from squirrel.artifact_manager.base import ArtifactManager, TmpArtifact
from squirrel.catalog import Catalog
from squirrel.catalog.catalog import CatalogSource, Source
from squirrel.serialization import JsonSerializer, MessagepackSerializer, SquirrelSerializer
from squirrel.store import FilesystemStore
from squirrel.store.filesystem import get_random_key

Serializers = {
    MessagepackSerializer: "messagepack",
    JsonSerializer: "jsonl",
}


class ArtifactFileStore(FilesystemStore):
    """
    A FilesystemStore serving as the backend for the FileSystemArtifactManager.

    The get and set methods are altered to allow for storing serialized data as well as raw files.
        If the final path component is a serializer name, the data is stored as a serialized file.
        If the final path component is "files", the data is stored as a raw file.
    """

    def complete_key(self, partial_key: Path, **open_kwargs) -> List[str]:
        """Returns a list of possible key continuations given a partial key."""
        full_path = Path(self.url, partial_key)
        if not self.fs.exists(full_path, **open_kwargs):
            return []
        return [str(Path(path).relative_to(full_path)) for path in self.fs.ls(full_path, detail=False, **open_kwargs)]

    def key_exists(self, key: Path, **open_kwargs) -> bool:
        """Checks if a key exists."""
        return self.fs.exists(Path(self.url) / key, **open_kwargs)

    def get(self, key: Path, mode: str = "rb", **open_kwargs) -> Any:
        """Retrieves an item with the given key."""
        if self.fs.exists(Path(self.url, key, Serializers[self.serializer.__class__]), **open_kwargs):
            # retrieve and deserialize data
            return super().get(str(Path(key, Serializers[self.serializer.__class__])), mode, **open_kwargs)
        elif self.fs.exists(Path(self.url, key, "files"), **open_kwargs):
            if "target" in open_kwargs:
                target_dir = str(open_kwargs.pop("target"))
                self.fs.makedirs(target_dir, exist_ok=True)
                self.fs.cp(str(Path(self.url, key, "files/*")), target_dir, recursive=True, **open_kwargs)
            # retrieve raw file
            else:
                return self.fs.cat(Path(self.url, key, "files"), **open_kwargs)
        else:
            raise ValueError(f"Key {key} does not exist!")

    def set(self, value: Any, key: Optional[Path] = None, mode: str = "wb", **open_kwargs) -> None:
        """Persists an item with the given key."""
        if key is None:
            key = get_random_key()
        # construct path under which to store the data
        if isinstance(value, Path):
            target = Path(self.url, key, "files", open_kwargs.pop("suffix", ""))
        else:
            target = Path(self.url, key, Serializers[self.serializer.__class__], open_kwargs.pop("suffix", ""))

        if self.fs.exists(target, **open_kwargs):
            raise ValueError(f"Key {key} already exists!")

        if isinstance(value, Path):
            if value.is_dir():
                self.fs.cp(str(value), str(target), recursive=True, **open_kwargs)
            else:
                self.fs.cp_file(value, target, **open_kwargs)
        else:
            super().set(value, str(target), mode, **open_kwargs)


class FileSystemArtifactManager(ArtifactManager):
    def __init__(
        self, url: str, serializer: Optional[SquirrelSerializer] = None, collection: str = "default", **fs_kwargs
    ):
        """
        Artifactmanager backed by fsspec filesystems.

        The manager logs artifacts according to the following file structure:
        url/collection/artifact/version/serializer/<content>
            url: root directory of the artifact store
            collection: the name of the collection defaults to 'default'
            artifact: (human-readable) name of the artifact
            version: version number starting at 1 which is automatically incremented
            serializer: the name of the serializer used to store the artifact (e.g. file, messagepack)
        """
        super().__init__(collection=collection)
        if serializer is None:
            serializer = JsonSerializer()
        self.backend = ArtifactFileStore(url=url, serializer=serializer, **fs_kwargs)

    def list_collection_names(self) -> Iterable:
        """List all collections managed by this ArtifactManager."""
        return self.backend.keys(nested=False)

    def exists_in_collection(self, artifact: str, collection: Optional[str] = None) -> bool:
        """Check if artifact exists in specified collection."""
        collection = collection or self.active_collection
        return self.backend.key_exists(Path(collection, artifact))

    def get_artifact_source(
        self,
        artifact: str,
        collection: Optional[str] = None,
        version: Optional[str] = None,
        catalog: Optional[Catalog] = None,
    ) -> CatalogSource:
        """Catalog entry for a specific artifact"""
        if catalog is None:
            catalog = Catalog()
        collection = collection or self.active_collection
        if version is None or version == "latest":
            version = f"v{max(int(vs[1:]) for vs in self.backend.complete_key(Path(collection) / Path(artifact)))}"
        if not self.backend.key_exists(Path(collection, artifact, version)):
            raise ValueError(f"Artifact {artifact} does not exist in collection {collection} with version {version}!")

        # TODO: Vary source (driver) description when support for serialised python values is added
        return CatalogSource(
            Source(
                driver_name="directory",
                driver_kwargs={
                    "url": Path(self.backend.url, collection, artifact, version, "files").as_uri(),
                    "storage_options": self.backend.storage_options,
                },
                metadata={
                    "collection": collection,
                    "artifact": artifact,
                    "version": version,
                },
            ),
            identifier=str(Path(collection, artifact)),
            catalog=catalog,
            version=int(version[1:]) + 1,  # Squirrel Catalog version is 1-based
        )

    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Provide catalog of all artifacts and their versions contained within specific collection"""
        collection = collection or self.active_collection
        catalog = Catalog()
        for artifact in self.backend.complete_key(Path(collection)):
            for version in self.backend.complete_key(Path(collection, artifact)):
                src = self.get_artifact_source(artifact, collection, version, catalog=catalog)
                catalog[str(Path(collection, artifact)), src.version] = src
        return catalog

    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log an arbitrary python object using store serialisation."""
        raise NotImplementedError(
            "Logging and retrieving python objects is not yet supported. Please serialize your"
            "objects and log resulting files with 'log_file' or 'log_folder'."
        )
        # Implementation for logging python objects can make use of
        # self.backend.set(obj, Path(collection, name, version))

    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None) -> Any:
        """Retrieve specific artifact value."""
        raise NotImplementedError(
            "Logging and retrieving python objects is not yet supported. Please serialize your"
            "objects and retrieve the logged files with 'download_artifact' instead."
        )

    def log_files(
        self,
        artifact_name: str,
        local_path: Path,
        collection: Optional[str] = None,
        artifact_path: Optional[Path] = None,
    ) -> CatalogSource:
        """Upload local file or folder to artifact store without serialisation"""
        if not isinstance(local_path, (str, Path)):
            raise ValueError("Path to file should be passed as a string or a pathlib.Path object!")
        local_path = Path(local_path)
        if not local_path.exists():
            raise ValueError(f"Provided local_path {local_path} does not exist!")

        if artifact_path is not None and not isinstance(artifact_path, (str, Path)):
            raise ValueError("Artifact path should be passed as a string or a pathlib.Path object!")

        collection = collection or self.active_collection

        version = f"v{len(self.backend.complete_key(Path(collection, artifact_name)))}"

        open_kwargs = {"auto_mkdir": True}
        if artifact_path is not None:
            open_kwargs["suffix"] = artifact_path
        self.backend.set(local_path, Path(collection, artifact_name, version), **open_kwargs)

        return self.get_artifact_source(artifact_name, collection)

    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[str] = None, to: Optional[Path] = None
    ) -> Union[Path, TmpArtifact]:
        """Download artifact to local path."""
        collection = collection or self.active_collection
        if version is None or version == "latest":
            version = f"v{max(int(vs[1:]) for vs in self.backend.complete_key(Path(collection) / Path(artifact)))}"

        if to is not None:
            if isinstance(to, str):
                to = Path(to)
            location = Path(collection, artifact, version)
            self.backend.get(Path(location), target=to / artifact)
            return to / artifact
        else:
            return TmpArtifact(self, collection, artifact, version)
