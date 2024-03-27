from squirrel.serialization.serializer import BaseSerializer


class ParquetSerializer(BaseSerializer):
    def file_extension(self) -> str:
        """File extension"""
        return "parquet"


class DeltalakeSerializer(ParquetSerializer):
    pass


class PolarsSerializer(ParquetSerializer):
    pass
