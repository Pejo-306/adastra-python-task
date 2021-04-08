from src.sources.data_source import DataSource


class FileDataSource(DataSource):

    def __init__(self, source_filepath: str) -> None:
        self.source_filepath = source_filepath
        self._source_file = None

    @property
    def is_open(self) -> bool:
        if self._source_file is None:
            return False
        return not self._source_file.closed

    def initialize(self) -> None:
        self._source_file = open(self.source_filepath, 'r')

    def has_message(self) -> bool:
        pass

    def read(self) -> dict:
        pass

    def close(self) -> None:
        self._source_file.close()
