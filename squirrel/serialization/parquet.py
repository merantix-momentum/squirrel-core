

from typing import Any, Iterable, List, Dict

from squirrel.serialization.serializer import SquirrelFileSerializer


class ParquetSerializer(SquirrelFileSerializer):

    def file_extension(self) -> str:
        """File extension"""
        return "parquet"
    
    def serialize_shard_to_file(self, obj: List[Dict[str, Any]], fp: str, **kwargs) -> None:
        raise NotImplementedError()
    
    def deserialize_shard_from_file(self, fp: str, **kwargs) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError()


class DeltalakeSerializer(ParquetSerializer):
    pass


class PolarsSerializer(ParquetSerializer):
    pass