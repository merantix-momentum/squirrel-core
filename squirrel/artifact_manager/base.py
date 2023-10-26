from abc import abstractmethod, ABC
from pathlib import Path
from typing import Optional, List, Union, Any, Iterable

from squirrel.catalog import Catalog, Source


class ArtifactManager(ABC):
    def __init__(self):
        """
        Artifact manager interface for various backends

        Maintains a mapping of artifact names to backend objects to facilitate logging and retrieval of arbitrary
        artifacts.
        """
        self._collection = "default"

    @property
    def collection(self) -> str:
        """
        Collections act as folders of artifacts.

        The manager maintains an 'active' collections which it logs to by default.
        To avoid incompatibility between different backends, collections cannot be nested (e.g. as subfolders on a
        filesystem).
        """
        return self._collection

    @collection.setter
    def collection(self, value: str) -> None:
        """Do not allow access to anything beyond the root location of the artifact store"""
        assert len(value) > 0 and "\\" not in value, "Invalid collection name - must not be empty and cannot be nested."
        self._collection = value

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
    def get_artifact_source(self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None) -> Source:
        """Catalog entry for a specific artifact"""
        raise NotImplementedError

    @abstractmethod
    def get_artifact_location(
        self, artifact_name: str, collection: Optional[str] = None, version: Optional[int] = None
    ) -> str:
        """Get full qualified path or wandb directory of artifact"""
        raise NotImplementedError

    @abstractmethod
    def log_file(self, local_path: Path, name: str, collection: Optional[str] = None) -> Source:
        """Upload file into (current) collection, increment version automatically"""

    @abstractmethod
    def log_collection(self, files: Union[Path, List[Path]], collection_name: Optional[str] = None) -> Catalog:
        """Upload folder or collection of files"""
        raise NotImplementedError

    @abstractmethod
    def log_object(self, obj: Any, name: str, collection: Optional[str] = None) -> Source:
        """Log an arbitrary python object"""
        raise NotImplementedError

    @abstractmethod
    def download_artifact(
        self, artifact: str, collection: Optional[str] = None, version: Optional[int] = None, to: Path = "./"
    ) -> Source:
        """Retrieve file (from current collection) to specific location. Retrieve latest version unless specified."""
        raise NotImplementedError

    @abstractmethod
    def download_collection(self, collection: Optional[str] = None, to: Path = "./") -> Catalog:
        """Retrieve file (from current collection) to specific location. Retrieve latest version of all artifacts."""
        raise NotImplementedError
