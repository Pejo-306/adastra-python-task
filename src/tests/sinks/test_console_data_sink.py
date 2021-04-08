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
        self.assertEqual(self.output_format, self.sink.output_format)

    def test_dump(self):
        message = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        expected_output = self.output_format.format(
            "A123", "15.6", "2020-10-07 13:28:43.399620+02:00"
        )
        with CaptureSTDOUT() as output:
            success = self.sink.dump(message)
        self.assertTrue(success)
        self.assertEqual(expected_output, output[0])

    def test_multiple_dumps(self):
        message1 = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        expected_output1 = self.output_format.format(
            "A123", "15.6", "2020-10-07 13:28:43.399620+02:00"
        )
        message2 = {
            "key": "B123",
            "value": "12.6",
            "ts": "2022-10-07 13:28:43.399620+02:00"
        }
        expected_output2 = self.output_format.format(
            "B123", "12.6", "2022-10-07 13:28:43.399620+02:00"
        )
        with CaptureSTDOUT() as output:
            success = self.sink.dump(message1)
            self.assertTrue(success)
            success = self.sink.dump(message2)
            self.assertTrue(success)
        self.assertEqual(expected_output1, output[0])
        self.assertEqual(expected_output2, output[1])
