import os
import json
from unittest import TestCase

from src.definitions import INPUT_FILES_DIR
from src.sources.data_source import DataSource
from src.sources.file_data_source import FileDataSource


class TestFileDataSource(TestCase):

    def test_object_creation(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath, 1024)
        self.assertIsInstance(source, FileDataSource)
        self.assertIsInstance(source, DataSource)
        self.assertEqual(source.source_filepath, source_filepath)
        self.assertEqual(source.chunk_size, 1024)

    def test_initialize(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            self.assertFalse(source.is_open)
            source.initialize()
            self.assertTrue(source.is_open)
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_has_message(self):
        # First, test with a file that has messages
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()
            self.assertTrue(source.has_message())
        finally:
            source.close()  # assumes FileDataSource.close() works

        # Second, test with a file that has no more messages
        source_filepath = os.path.join(INPUT_FILES_DIR, "no_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()
            self.assertFalse(source.has_message())
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_read(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()

            message = source.read()
            self.assertIn("key", message)
            self.assertEqual(message["key"], "A123")
            self.assertIn("value", message)
            self.assertEqual(message["value"], "15.6")
            self.assertIn("ts", message)
            self.assertEqual(message["ts"], "2020-10-07 13:28:43.399620+02:00")
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_multiple_reads(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "multiple_messages.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()

            message = source.read()  # first message
            self.assertIn("key", message)
            self.assertEqual(message["key"], "A123")
            self.assertIn("value", message)
            self.assertEqual(message["value"], "15.6")
            self.assertIn("ts", message)
            self.assertEqual(message["ts"], "2020-10-07 13:28:43.399620+02:00")

            message = source.read()  # second message
            self.assertIn("key", message)
            self.assertEqual(message["key"], "B123")
            self.assertIn("value", message)
            self.assertEqual(message["value"], "12.6")
            self.assertIn("ts", message)
            self.assertEqual(message["ts"], "2022-10-07 13:28:43.399620+02:00")
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_close(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        source.initialize()
        self.assertTrue(source.is_open)
        source.close()
        self.assertFalse(source.is_open)

    def test_load_chunk(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()
            self.assertEqual(len(source._loaded_chunk), 0)
            source._load_chunk()
            self.assertEqual(len(source._loaded_chunk), 1)
            self.assertEqual(source._loaded_chunk[0],
                             '{"key": "A123", "value": "15.6", "ts": "2020-10-07 13:28:43.399620+02:00"}')
        finally:
            source.close()

    def test_split_json_chunk_with_complete_chunk(self):
        chunk = '  [  {"key": "11": "value": "111"} , {"key": "22": "value": "222"} , {"key": "33": "value": "333"} ]  '
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], '{"key": "11": "value": "111"}')
        self.assertEqual(result[1], '{"key": "22": "value": "222"}')
        self.assertEqual(result[2], '{"key": "33": "value": "333"}')

    def test_split_json_chunk_with_empty_chunk(self):
        chunk = ""
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(len(result), 0)
        chunk = " [     ] "
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(len(result), 0)

    def test_split_json_chunk_with_incomplete_chunk(self):
        chunk = '  [  {"key": "11": "value": "111"} , {"key": "22": "value": "222"} , {"key": "33": "val'
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], '{"key": "11": "value": "111"}')
        self.assertEqual(result[1], '{"key": "22": "value": "222"}')
        self.assertEqual(result[2], '{"key": "33": "val')
        with self.assertRaises(json.JSONDecodeError):
            json.loads(result[2])

    def test_split_json_chunk_with_prepend(self):
        prepend = '{"key": "33", "val'
        chunk = 'ue": "333"} , {"key": "44", "value": "444"}  ]  '
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'ue": "333"}')
        with self.assertRaises(json.JSONDecodeError):
            json.loads(result[0])
        self.assertEqual(result[1], '{"key": "44", "value": "444"}')

        result = tuple(FileDataSource._split_json_chunk(chunk, prepend))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], '{"key": "33", "value": "333"}')
        self.assertEqual(result[1], '{"key": "44", "value": "444"}')
