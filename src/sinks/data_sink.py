from abc import ABC, abstractmethod


class DataSink(ABC):
    """Interface for container where data can be arbitrarily dumped

    Methods:
        initialize(): prepare the data sink for the incoming data dumps
        dump(message): dump a single message into the sink
        close(): clean up the sink and terminate the connection to it
    """

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
