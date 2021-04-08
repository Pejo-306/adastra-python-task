from src.sinks.data_sink import DataSink


class ConsoleDataSink(DataSink):
    """Data sink which dumps its messages to the console (STDOUT)

    Attributes:
        output_format(str): string format of the outputted message

    Methods:
        __enter__(): (see DataSink)
        __exit__(): (see DataSink)
        initialize(): do nothing (see DataSink)
        dump(message): print a single formatted message on the console (STDOUT)
        close(): do nothing (see DataSink)
    """

    def __init__(self, output_format: str) -> None:
        """Construct console data sink

        :param output_format: string format of outputted message
        :type output_format: str
        """
        self.output_format = output_format

    def __enter__(self):
        """Do nothing (see DataSink and ConsoleDataSink.initialize())"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Do nothing (see DataSink and ConsoleDataSink.close())"""
        pass

    def initialize(self) -> None:
        """Do nothing (see DataSink)

        Console data sinks do not require any preemptive setup.
        """
        pass

    def dump(self, message: dict) -> bool:
        """Print a single message on the console

        Message bodies must be mappings which contain the following keys:
        "key", "value" and "ts". Any other keys in the mapping are ignored.
        The outputted message is formatted with the string format which is
        initially supplied when the data sink is created.

        :param message: body of the message
        :type message: dict

        :return: status which indicates whether the dump was successful
        :rtype: bool
        """
        output = self.output_format.format(message["key"],
                                           message["value"],
                                           message["ts"])
        print(output)
        return True

    def close(self) -> None:
        """Do nothing (see DataSink)

        Console data sinks do not require any cleanup/termination.
        """
        pass
