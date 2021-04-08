from __future__ import annotations  # introduced in Python 3.10+

from src.sources.data_source import DataSource
from src.sinks.data_sink import DataSink


class ETL:

    def __init__(self):
        self.data_source = None
        self.data_sink = None

    def source(self, source_cls: DataSource, *args, **kwargs) -> ETL:
        self.data_source = source_cls(*args, **kwargs)
        return self

    def sink(self, sink_cls: DataSink, *args, **kwargs) -> ETL:
        self.data_sink = sink_cls(*args, **kwargs)
        return self

    def run(self) -> None:
        with self.data_source, self.data_sink:
            while self.data_source.has_message():
                message = self.data_source.read()
                self.data_sink.dump(message)
