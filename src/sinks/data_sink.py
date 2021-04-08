from abc import ABC, abstractmethod


class DataSink(ABC):
    """Interface for container where data can be arbitrarily dumped

    Methods:
        __enter__(): context manager entrance; ensure proper sink initialization
        __exit__(): context manager exit; ensure proper sink termination
        initialize(): prepare the data sink for the incoming data dumps
        dump(message): dump a single message into the sink
        close(): clean up the sink and terminate the connection to it
    """

    @abstractmethod
    def __enter__(self):
        """Ensure proper sink initialization"""
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper sink termination"""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """Prepare the data sink for the incoming data dumps"""
        pass

    @abstractmethod
    def dump(self, message: dict) -> bool:
        """Dump a single message into the sink

        :param message: body of the message
        :type message: dict

        :return: status which indicates whether the dump was successful
        :rtype: bool
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up the sink and terminate the connection to it"""
        pass
