import os
from unittest import TestCase

from src.definitions import INPUT_FILES_DIR
from src.etl import ETL
from src.sources.data_source import DataSource
from src.sources.file_data_source import FileDataSource
from src.sinks.data_sink import DataSink
from src.sinks.console_data_sink import ConsoleDataSink
from src.tests.test_helpers.capture_stdout import CaptureSTDOUT


class TestETL(TestCase):

    def test_object_construction(self):
        etl = ETL()
        self.assertIsNone(etl.data_source)
        self.assertIsNone(etl.data_sink)

    def test_source(self):
        source_filepath = "/path/to/file.json"
        chunk_size = 1024
        etl = ETL()
        etl.source(FileDataSource, source_filepath, chunk_size)
        self.assertIsNotNone(etl.data_source)
        self.assertIsInstance(etl.data_source, DataSource)
        self.assertIsInstance(etl.data_source, FileDataSource)
        self.assertEqual(source_filepath, etl.data_source.source_filepath)
        self.assertEqual(chunk_size, etl.data_source.chunk_size)

    def test_sink(self):
        output_format = "key: {} | value: {} | ts: {}"
        etl = ETL()
        etl.sink(ConsoleDataSink, output_format)
        self.assertIsNotNone(etl.data_sink)
        self.assertIsInstance(etl.data_sink, DataSink)
        self.assertIsInstance(etl.data_sink, ConsoleDataSink)
        self.assertEqual(output_format, etl.data_sink.output_format)

    def test_run(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        sink_output_format = "key: {} | value: {} | ts: {}"
        expected_output = "key: A123 | value: 15.6 | ts: 2020-10-07 13:28:43.399620+02:00"
        etl = ETL()
        etl.source(FileDataSource, source_filepath)
        etl.sink(ConsoleDataSink, sink_output_format)
        with CaptureSTDOUT() as output:
            etl.run()
        self.assertEqual(1, len(output))
        self.assertEqual(expected_output, output[0])

    def test_method_chaining(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        sink_output_format = "key: {} | value: {} | ts: {}"
        expected_output = "key: A123 | value: 15.6 | ts: 2020-10-07 13:28:43.399620+02:00"
        etl = ETL()
        self.assertIsInstance(etl.source(FileDataSource, source_filepath), ETL)
        self.assertIsInstance(etl.sink(ConsoleDataSink, sink_output_format), ETL)
        del etl  # explicitly not required anymore
        with CaptureSTDOUT() as output:
            ETL().source(FileDataSource, source_filepath).sink(ConsoleDataSink, sink_output_format).run()
        self.assertEqual(1, len(output))
        self.assertEqual(expected_output, output[0])
