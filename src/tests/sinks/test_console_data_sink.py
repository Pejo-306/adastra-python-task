from unittest import TestCase

from src.sinks.console_data_sink import ConsoleDataSink
from src.sinks.data_sink import DataSink
from src.tests.test_helpers.capture_stdout import CaptureSTDOUT


class TestConsoleDataSink(TestCase):

    def setUp(self):
        self.output_format = "key: {} | value: {} | ts: {}"
        self.sink = ConsoleDataSink(self.output_format)

    def test_object_construction(self):
        self.assertIsInstance(self.sink, ConsoleDataSink)
        self.assertIsInstance(self.sink, DataSink)
        self.assertEquals(self.sink.output_format, self.output_format)

    def test_dump(self):
        message = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        expected_output = self.output_format.format("A123",
                                                    "15.6",
                                                    "2020-10-07 13:28:43.399620+02:00")
        with CaptureSTDOUT() as output:
            success = self.sink.dump(message)
        self.assertTrue(success)
        self.assertEquals(output[0], expected_output)
