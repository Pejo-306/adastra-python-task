from src.sinks.data_sink import DataSink


class ConsoleDataSink(DataSink):

    def __init__(self, output_format: str) -> None:
        pass

    def initialize(self) -> None:
        pass

    def dump(self, message: dict) -> bool:
        pass

    def close(self) -> None:
        pass
