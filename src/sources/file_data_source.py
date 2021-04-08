import json
from collections import deque
from typing import Iterator
from io import StringIO

from src.sources.data_source import DataSource
from src.exceptions.file_not_open_error import FileNotOpenError
from src.exceptions.file_source_depleted import FileSourceDepleted


class FileDataSource(DataSource):
    """Data source which retrieves messages from a JSON file

    This data source retrieves messages from a specified source JSON file. The
    latter's content must be valid JSON and must contain exactly one JSON array
    of JSON objects, separated by commas, with the following predefined structure:
        `{"key": <value>, "value": <value>, "ts": <value>}`

    The JSON file is lazily read in chunks with predefined size (in bytes). After
    a text chunk has been read, it is split into several strings, each representing
    a message, which are pushed into an internal queue. Any leftover text data that
    does not complete a full JSON object by itself, i.e. is not a full message, is
    written to a string stream and later prepended to the next loaded chunk.

    Every time the data source is queried for a new message the leftmost string
    message of the internal queue is popped and deserialized from JSON to a Python
    dictionary.

    Attributes:
        source_filepath(str): path to source JSON file
        chunk_size(int): size of string chunks, read from the source file
        _source_file(TextIOWrapper): text stream from source JSON file
        _loaded_messages(deque): queue of preloaded string messages
        _text_chunk_prepend(StringIO): string stream, used to prepend incomplete
                                       string messages to the next chunk of data
        _finished_reading(bool): true if the source file is depleted, false otherwise

    Properties:
        is_open(bool): true if source JSON file has been opened, false otherwise

    Methods:
        __enter__(): (see DataSource)
        __exit__(): (see DataSource)
        initialize(): open the source file in 'read' mode
        has_message(): indicate whether there is an available message for extraction
        read(): extract and deserialize a single JSON message
        close(): close the source file
        _load_chunk(): load the next chunk of text data from the source file

    Static methods:
        _split_json_chunk(chunk, prepend): properly split a JSON text chunk into
                                           multiple strings of JSON objects
    """

    def __init__(self, source_filepath: str, chunk_size: int = 256) -> None:
        """Construct file data source

        :param source_filepath: path to source JSON file
        :type source_filepath: str
        :param chunk_size: size of text chunks, in bytes
        :type chunk_size: int
        """
        self.source_filepath = source_filepath
        self.chunk_size = chunk_size
        self._source_file = None
        self._loaded_messages = deque()
        self._text_chunk_prepend = StringIO()
        self._finished_reading = False

    def __enter__(self):
        """Ensure proper initialization of file data source"""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper termination of file data source"""
        self.close()

    @property
    def is_open(self) -> bool:
        """True if source JSON file has been opened, false otherwise"""
        if self._source_file is None:
            return False
        return not self._source_file.closed

    def initialize(self) -> None:
        """Open the source JSON file in 'read' mode"""
        self._source_file = open(self.source_filepath, 'r')

    def has_message(self) -> bool:
        """Indicate whether there is an available message for extraction

        Note that if the internal message queue is empty, an attempt is made to
        preemptively load the next text chunk from the source file.

        :raises FileNotOpenError: the source file must be opened

        :return: status which indicates an available message
        :rtype: bool
        """
        if not self.is_open:
            raise FileNotOpenError(self.source_filepath)

        if len(self._loaded_messages) == 0:  # preemptively load the next chunk
            self._load_chunk()
        return True if len(self._loaded_messages) > 0 else False

    def read(self) -> dict:
        """Extract and deserialize a single JSON message

        When the data source is queried for a message, leftmost string of the
        internal message queue is extracted and an attempt is made to deserialize
        it from a JSON object to a Python dictionary. If the extracted string
        message is incomplete or the internal message queue is empty, text chunks
        are continuously loaded from the source file until a proper message is
        constructed.

        :raises FileNotOpenError: the source file must be opened
        :raises FileSourceDepleted: when reading is attempted on a depleted source file

        :return: body of the extracted message
        :rtype: dict
        """
        if not self.is_open:
            raise FileNotOpenError(self.source_filepath)

        while True:  # keep loading chunks until a proper message is constructed
            if len(self._loaded_messages) == 0:  # load the next chunk
                self._load_chunk()
                if len(self._loaded_messages) == 0:
                    raise FileSourceDepleted(self.source_filepath)

            text_message = self._loaded_messages.popleft()  # retrieve the next message
            try:
                message = json.loads(text_message)
            except json.JSONDecodeError:  # read text chunk cannot be parsed as valid json
                self._text_chunk_prepend.write(text_message)
            else:
                return message

    def close(self) -> None:
        """Close the source JSON file"""
        self._source_file.close()

    def _load_chunk(self) -> None:
        """Load the next chunk of text data from the source file

        Any incomplete JSON text data from previous chunks is prepended to the
        current chunk. Afterwards, the resulting text chunk is split into multiple
        string JSON objects which are pushed to the internal message queue.

        When the source file is fully read a boolean flag is set, indicating that
        the data source has finished reading and will not try to load any more
        chunks in the future.
        """
        if not self._finished_reading:
            text_chunk = self._source_file.read(self.chunk_size)
            if len(text_chunk) < self.chunk_size:  # file has been fully read (reached EOF)
                self._finished_reading = True
            self._loaded_messages.extend(self._split_json_chunk(text_chunk, self._text_chunk_prepend.getvalue()))
            # flush text chunk string stream
            self._text_chunk_prepend.truncate(0)
            self._text_chunk_prepend.seek(0)

    @staticmethod
    def _split_json_chunk(chunk: str, prepend: str = "") -> Iterator[str]:
        """Properly split a JSON text chunk into multiple strings of JSON objects

        The JSON objects must be encased in '{}' parentheses and separated by commas.
        Whitespace characters are stripped and array brackets ('[]') are ignored.
        If the last JSON object in the text chunk is incomplete, its incomplete text
        is always available as the last string in the returned iterable.

        :param chunk: text chunk of fixed length
        :type chunk: str
        :param prepend: incomplete string data from previous chunk
        :type prepend: str

        :return: iterable sequence of string JSON objects
        :rtype: Iterator[str]
        """
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
