import re
from abc import abstractmethod, ABC
from pathlib import Path
from typing import Optional, List, Any, Iterable

from squirrel.catalog import Catalog, Source


class ArtifactManager(ABC):
    def __init__(self):
        """
        Artifact manager interface for various backends

        Maintains a mapping of artifact names to backend objects to facilitate logging and retrieval of arbitrary
        artifacts.
        """
        self._active_collection = "default"

    @property
    def active_collection(self) -> str:
        """
        Collections act as folders of artifacts.

        It is ultimately up to the user how to structure their artifact store and the collections therein. All
        operations accessing artifacts allow explicit specification of the collection to use.
        Therefore, users could use collections to separate different artifact types or different experiments / runs.
        To facilitate the latter use case in particular, the manager maintains an 'active' collection which it logs to
        by default. This can be set once at the run start when the manager is initialized and then left unchanged.

        To avoid incompatibility between different backends, collections cannot be nested (e.g. as subfolders on a
        filesystem) as in particular the WandB backend has no real notion of nested folder structures.
        """
        return self._active_collection

    @active_collection.setter
    def active_collection(self, value: str) -> None:
        """
        Sets the active collections that is being logged to by default.

        The provided values is verified to ensure that no nested collections are used.
        """
        if not re.match(r"^[a-zA-Z0-9\-_:]+$", value):
            raise ValueError(
                "Invalid collection name - must not be empty and can only contain alphanumerics, dashes, underscores "
                "and colons."
            )
        self._active_collection = value

    @abstractmethod
    def list_collection_names(self) -> Iterable:
        """Return list of all collections in the artifact store"""
        raise NotImplementedError

    @abstractmethod
    def store_to_catalog(self) -> Catalog:
        """Provide a catalog of all stored artifacts."""
        raise NotImplementedError

    @abstractmethod
    def collection_to_catalog(self, collection: Optional[str] = None) -> Catalog:
        """Catalog of all artifacts within a specific collection."""

    @abstractmethod
    def get_artifact(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None) -> Any:
        """Retrieve specific artifact value."""
        raise NotImplementedError

    @abstractmethod
    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        """Upload file into (current) collection, increment version automatically"""
        raise NotImplementedError

    @abstractmethod
    def log_files(self, local_paths: List[Path], collection: Optional[str] = None) -> Catalog:
        """Upload a collection of files into a (current) collection"""
        raise NotImplementedError

    @abstractmethod
    def log_folder(self, files: Path, collection: Optional[str] = None) -> Catalog:
        """Upload folder as collection of artifacts into store"""
        raise NotImplementedError

    @abstractmethod
    def log_artifact(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """
        Log an arbitrary python object

        The serialisation method used is backend dependent. When using a simple FileStore backend any SquirrelSerializer
        can be chosen. For WandB objects serialisation is handled by WandB itself.
        """
        raise NotImplementedError

    @abstractmethod
    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None, to: Path = "./"
    ) -> Source:
        """Retrieve file (from current collection) to specific location. Retrieve latest version unless specified."""
        raise NotImplementedError

    @abstractmethod
    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        """Retrieve files (from current collection) to specific location. Retrieve latest version of all artifacts."""
        raise NotImplementedError
