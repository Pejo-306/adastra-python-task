from abc import ABC, abstractmethod


class DataSource(ABC):
    """Interface for container from which data can be arbitrarily extracted

    Methods:
        __enter__(): context manager entrance; ensure proper source initialization
        __exit__(): context manager exit; ensure proper source termination
        initialize(): prepare the data source for message extraction
        has_message(): indicate whether there is an available message for extraction
        read(): extract a single message
        close(): terminate the connection to the data source
    """

    @abstractmethod
    def __enter__(self):
        """Ensure proper source initialization"""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper source termination"""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """Prepare the data source for message extraction"""
        pass

    @abstractmethod
    def has_message(self) -> bool:
        """Indicate whether there is an available message for extraction

        :return: status which indicates an available message
        :rtype: bool
        """
        pass

    @abstractmethod
    def read(self) -> dict:
        """Extract a single message from the data source

        :return: body of the extracted message
        :rtype: dict
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up the source and terminate the connection to it"""
        pass
