from src.sources.data_source import DataSource


class FileDataSource(DataSource):

    def __init__(self, source_filepath: str) -> None:
        pass

    @property
    def is_open(self) -> bool:
        pass

    def initialize(self) -> None:
        pass

    def has_message(self) -> bool:
        pass

    def read(self) -> dict:
        pass

    def close(self) -> None:
        pass
