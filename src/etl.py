from __future__ import annotations  # introduced in Python 3.10+

from src.sources.data_source import DataSource
from src.sinks.data_sink import DataSink


class ETL:
    """Pseudo Extract,Transform,Load (ETL) interface

    This interface relies on a common interface for data sources - 'DataSource',
    and a common interface for data sinks - 'DataSink'. Attempting to run the
    ETL process with any sources or sinks that do not implement the aforementioned
    interfaces will result in undefined behavior. (see DataSource and DataSink for
    more details)

    Data is extracted in the form of singular messages from the data source.
    Afterwards, the message is immediately passed to the data sink, i.e. it is
    transmitted on a one by one basis. This ETL does not perform any analysis,
    manipulation, buffering, etc. of the received data.

    Attributes:
        data_source(DataSource): instance of the data source
        data_sink(DataSink): instance of the data sink

    Methods:
        source(source_cls, *args, **kwargs): create an instance of a chosen type
                                             of data source
        sink(sink_cls, *args, **kwargs): create an instance of a chosen type
                                         of data sink
        run(): extract messages from the source and dump them in the sink
    """

    def __init__(self):
        """Construct ETL instance"""
        self.data_source = None
        self.data_sink = None

    def source(self, source_cls: DataSource, *args, **kwargs) -> ETL:
        """Instantiate a data source and save a reference to it

        :param source_cls: class of data source
        :type: DataSource
        :param args: arguments to data source constructor
        :type: tuple
        :param kwargs: keyword arguments to data source constructor
        :type: dict

        :return: reference to self
        :rtype: ETL
        """
        self.data_source = source_cls(*args, **kwargs)
        return self

    def sink(self, sink_cls: DataSink, *args, **kwargs) -> ETL:
        """Instantiate a data sink and save a reference to it

        :param sink_cls: class of data sink
        :type: DataSink
        :param args: arguments to data sink constructor
        :type: tuple
        :param kwargs: keyword arguments to data sink constructor
        :type: dict

        :return: reference to self
        :rtype: ETL
        """
        self.data_sink = sink_cls(*args, **kwargs)
        return self

    def run(self) -> None:
        """Extract messages from the source and dump them in the sink

        The data source and data sink are properly initialized and terminated
        via context management.
        Messages are read and transmitted on a one by one basis.
        """
        with self.data_source, self.data_sink:
            while self.data_source.has_message():
                message = self.data_source.read()
                self.data_sink.dump(message)
