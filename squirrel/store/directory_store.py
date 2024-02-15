import typing as t

from squirrel.serialization.serializer import SquirrelSerializer
from squirrel.store.filesystem import FilesystemStore, get_random_key


class DirectoryStore(FilesystemStore):
    def __init__(
        self, url: str, serializer: t.Optional[SquirrelSerializer] = None, clean: bool = False, **storage_options
    ) -> None:
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
    