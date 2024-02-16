import typing as t

from squirrel.serialization.serializer import SquirrelFileSerializer
from squirrel.store.filesystem import FilesystemStore, get_random_key


class DirectoryStore(FilesystemStore):
    def __init__(self, url: str, serializer: SquirrelFileSerializer, clean: bool = False, **storage_options) -> None:
        """A Store to access files in a potentially nested directory
        
        If the directory contains files with different file extensions, only the ones that match
        the file extension of the `serializer` are considered, the rest will simply be ignored.

        Args:
            url (str): Path to the root directory. If this path does not exist, it will be created.
            serializer (SquirrelSerializer): Serializer that is used to serialize data before persisting (see
                :py:meth:`set`) and to deserialize data after reading (see :py:meth:`get`). 
            clean (bool): If true, all files in the store will be removed recursively
            **storage_options: Keyword arguments passed to filesystem initializer.
        """
        super().__init__(url=url, serializer=serializer, clean=clean, storage_options=storage_options)

    def set(self, value: t.Any, key: str = None) -> None:
        """
        Store the `value` using the method provided by SquirrelSerializer.serialize_shard_to_file
        """
        if key is None:
            key = get_random_key()
        self.serializer().serialize_shard_to_file(obj=value, fp=f"{self.url}/{key}")

    def get(self, key: str) -> t.Any:
        """
        retrieve the value of the key using SquirrelSerializer.deserialize_shard_from_file
        """
        return self.serializer().deserialize_shard_from_file(fp=f"{self.url}/{key}")

    def keys(self, nested: bool = True, **kwargs) -> t.Iterator[str]:
        yield from super().keys(
            nested=nested, regex_filter=f"{self.serializer.file_extension}", **kwargs
        )
