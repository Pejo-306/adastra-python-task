from collections import deque
from typing import Iterator

from src.sources.data_source import DataSource


class FileDataSource(DataSource):

    def __init__(self, source_filepath: str, chunk_size: int = 256) -> None:
        self.source_filepath = source_filepath
        self.chunk_size = chunk_size
        self._source_file = None
        self._loaded_chunk = deque()
        self._finished_reading = False

    @property
    def is_open(self) -> bool:
        if self._source_file is None:
            return False
        return not self._source_file.closed

    def initialize(self) -> None:
        self._source_file = open(self.source_filepath, 'r')

    def has_message(self) -> bool:
        if not self.is_open:
            pass  # TODO: raise exception here

        if len(self._loaded_chunk) == 0:  # preemptively load the next chunk
            self._load_chunk()
        return True if len(self._loaded_chunk) > 0 else False

    def read(self) -> dict:
        if not self.is_open:
            pass  # TODO: raise exception here

    def close(self) -> None:
        self._source_file.close()

    def _load_chunk(self) -> None:
        if not self._finished_reading:
            text_chunk = self._source_file.read(self.chunk_size)
            if len(text_chunk) < self.chunk_size:  # file has been fully read (reached EOF)
                self._finished_reading = True
            self._loaded_chunk.extend(self._split_json_chunk(text_chunk))

    @staticmethod
    def _split_json_chunk(chunk: str, prepend: str = "") -> Iterator[str]:
        # Split the json chunk into multiple json strings
        chunk = (prepend + chunk).strip()
        if len(chunk) == 0:
            return []
        result = []
        in_json_message = False
        start_index = 0
        for i in range(len(chunk)):
            if chunk[i] == '{':
                in_json_message = True
            elif chunk[i] == '}':
                in_json_message = False
            elif chunk[i] == ',' and not in_json_message:
                result.append(chunk[start_index:i])
                start_index = i + 1
        result.append(chunk[start_index:i+1])

        # Clean up the result
        result = map(lambda s: s.replace('[', '', 1).replace(']', '', 1).strip(), result)
        return (s for s in result if len(s) > 0)
