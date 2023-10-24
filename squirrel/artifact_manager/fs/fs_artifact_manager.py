from squirrel.artifact_manager.base import ArtifactManager
from squirrel.serialization import MessagepackSerializer
from squirrel.store import FilesystemStore


class FileSystemArtifactManager(ArtifactManager):
    def __init__(self, url: str):
        """Artifact manager based by local or cloud filesystem."""
        # TODO serialise to deltalake instead of messagepack underneath
        # TODO introduce buffer logic to avoid single entry additions into deltalake
        fs_store = FilesystemStore(url=url, serializer=MessagepackSerializer())
        super().__init__(fs_store)

    # TODO allow schema to be configured at initialisation
