from abc import abstractmethod, ABC
from pathlib import Path
from typing import Optional, List, Union, Iterable, Tuple

from squirrel.catalog import Catalog, Source
from squirrel.catalog.catalog import CatalogKey
from squirrel.iterstream import Composable
from squirrel.store import AbstractStore


class ArtifactManager(ABC):
    def __init__(self, backend_store: AbstractStore):
        self.backend: AbstractStore = backend_store
        self._collection = "default"

    @abstractmethod
    def _key_to_artifact(self, key: str) -> Tuple[str, str, str]:
        """Transform key of underlying store to (collection, artifact, version) tuple."""
        raise NotImplementedError

    @abstractmethod
    def _artifact_to_key(self, collection: str, artifact: str, version: str) -> str:
        """Transform (collection, artifact, version) tuple to key of underlying store."""
        raise NotImplementedError

    @abstractmethod
    def get_artifact_location(self, artifact, collection: Optional[str] = None, version: Optional[int] = None) -> str:
        """Get full qualified path or wandb directory of artifact"""
        raise NotImplementedError

    @property
    def collection(self) -> str:
        """
        Collections act as folders of artifacts.

        The manager maintains an 'active' collections which it logs to by default.
        Depending on the backend, the collection can also point to a nested collection
        (e.g. a subfolder on a filesystem).
        """
        return self._collection

    @collection.setter
    def collection(self, value: str) -> None:
        """Do not allow access to anything beyond the root location of the artifact store"""
        assert len(value) > 0 and "\\" not in value, "Invalid collection name - must not be empty and cannot be nested."
        self._collection = value

    def get_catalog(self) -> Catalog:
        """Return catalog of artifacts"""
        cat = Catalog()
        for key in self.backend.keys():
            collection, name, version = self._key_to_artifact(key)
            cat[CatalogKey(f"{collection}/{name}", int(version))] = Source("file", ...)
        return cat

    def get_collections(self) -> List[str]:
        return list(set([self._key_to_artifact(entry)[0] for entry in self.backend.keys()]))

    def get_artifacts(self) -> Iterable:
        """Iterator over all known artifacts"""
        return Composable(self.backend.keys()).map(self._key_to_artifact)

    def get_collection_artifacts(self, collection: Optional[str] = None) -> List[str]:
        """List all artifacts belonging to specific collection"""
        if collection is None:
            collection = self.collection
        raise Composable(self.backend.keys()).map(self._key_to_artifact).filter(lambda x: x[0] == collection)

    def get_artifact_versions(self, artifact, collection: Optional[str] = None) -> List[str]:
        """Get all available versions of artifact"""
        raise NotImplementedError

    def fetch_artifact(
        self, name: str, collection: Optional[str] = None, version: Optional[int] = None, to: Path = "./"
    ):
        """Retrieve file (from current collection) to specific location. Retrieve latest version unless specified."""
        raise NotImplementedError

    def fetch_collection(self, collection: Optional[str] = None, to: Path = "./"):
        """Retrieve file (from current collection) to specific location. Retrieve latest version unless specified."""
        raise NotImplementedError

    def log_artifact(self, local_path: Path, name: str, collection: Optional[str] = None) -> None:
        """Upload file into (current) collection, increment version automatically"""
        raise NotImplementedError

    def log_collection(self, files: Union[Path, List[Path]], collection_name: Optional[str] = None) -> None:
        """Upload folder or collection of files"""
        raise NotImplementedError
