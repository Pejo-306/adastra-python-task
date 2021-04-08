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

    @staticmethod
    def _split_json_chunk(chunk: str, prepend: str = ""):
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
