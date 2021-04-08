from __future__ import annotations  # obsolete if using Python 3.10+

from src.sources.data_source import DataSource
from src.sinks.data_sink import DataSink


class ETL:

    def source(self, source_cls: DataSource, *args, **kwargs) -> ETL:
        pass

    def sink(self, sink_cls: DataSink, *args, **kwargs) -> ETL:
        pass

    def run(self) -> None:
        pass
