from src.sinks.data_sink import DataSink


class ConsoleDataSink(DataSink):

    def __init__(self, output_format: str) -> None:
        self.output_format = output_format

    def initialize(self) -> None:
        pass

    def dump(self, message: dict) -> bool:
        output = self.output_format.format(message["key"],
                                           message["value"],
                                           message["ts"])
        print(output, end="")
        return True

    def close(self) -> None:
        pass
