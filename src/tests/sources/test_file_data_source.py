import os
import json
from unittest import TestCase

from src.definitions import INPUT_FILES_DIR
from src.sources.data_source import DataSource
from src.sources.file_data_source import FileDataSource
from src.exceptions.file_not_open_error import FileNotOpenError
from src.exceptions.file_source_depleted import FileSourceDepleted


class TestFileDataSource(TestCase):

    def test_object_creation(self):
        chunk_size = 1024
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath, chunk_size)
        self.assertIsInstance(source, FileDataSource)
        self.assertIsInstance(source, DataSource)
        self.assertEqual(source_filepath, source.source_filepath)
        self.assertEqual(chunk_size, source.chunk_size)

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

    def test_has_message_on_unopened_file(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        with self.assertRaises(FileNotOpenError) as context:
            source.has_message()
        self.assertEqual(source_filepath, context.exception.filepath)

    def test_read(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()

            message = source.read()
            self.assertIn("key", message)
            self.assertEqual("A123", message["key"])
            self.assertIn("value", message)
            self.assertEqual("15.6", message["value"])
            self.assertIn("ts", message)
            self.assertEqual("2020-10-07 13:28:43.399620+02:00", message["ts"])
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_read_with_small_chunk_size(self):
        small_chunk_size = 8
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath, small_chunk_size)
        self.assertEqual(small_chunk_size, source.chunk_size)
        try:
            source.initialize()

            message = source.read()
            self.assertIn("key", message)
            self.assertEqual("A123", message["key"])
            self.assertIn("value", message)
            self.assertEqual("15.6", message["value"])
            self.assertIn("ts", message)
            self.assertEqual("2020-10-07 13:28:43.399620+02:00", message["ts"])
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_multiple_reads(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "multiple_messages.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()

            message = source.read()  # first message
            self.assertIn("key", message)
            self.assertEqual("A123", message["key"])
            self.assertIn("value", message)
            self.assertEqual("15.6", message["value"])
            self.assertIn("ts", message)
            self.assertEqual("2020-10-07 13:28:43.399620+02:00", message["ts"])

            message = source.read()  # second message
            self.assertIn("key", message)
            self.assertEqual("B123", message["key"])
            self.assertIn("value", message)
            self.assertEqual("12.6", message["value"])
            self.assertIn("ts", message)
            self.assertEqual("2022-10-07 13:28:43.399620+02:00", message["ts"])
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_read_on_unopened_file(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        with self.assertRaises(FileNotOpenError) as context:
            source.read()
        self.assertEqual(source_filepath, context.exception.filepath)

    def test_read_on_depleted_file_source(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()
            self.assertTrue(source.has_message())
            source.read()
            self.assertFalse(source.has_message())
            with self.assertRaises(FileSourceDepleted) as context:
                source.read()
            self.assertEqual(source_filepath, context.exception.filepath)
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_close(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        source.initialize()
        self.assertTrue(source.is_open)
        source.close()
        self.assertFalse(source.is_open)

    def test_context_manager(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        with FileDataSource(source_filepath) as source:
            self.assertIsInstance(source, FileDataSource)
            self.assertTrue(source.is_open)
        self.assertFalse(source.is_open)

    def test_load_chunk(self):
        chunk_size = 1024
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath, chunk_size)
        try:
            source.initialize()
            self.assertEqual(0, len(source._loaded_messages))
            source._load_chunk()
            self.assertEqual(1, len(source._loaded_messages))
            self.assertEqual('{"key": "A123", "value": "15.6", "ts": "2020-10-07 13:28:43.399620+02:00"}',
                             source._loaded_messages[0])
        finally:
            source.close()

    def test_split_json_chunk_with_complete_chunk(self):
        chunk = '  [  {"key": "11": "value": "111"} , {"key": "22": "value": "222"} , {"key": "33": "value": "333"} ]  '
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(3, len(result))
        self.assertEqual('{"key": "11": "value": "111"}', result[0])
        self.assertEqual('{"key": "22": "value": "222"}', result[1])
        self.assertEqual('{"key": "33": "value": "333"}', result[2])

    def test_split_json_chunk_with_empty_chunk(self):
        chunk = ""
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(0, len(result))
        chunk = " [     ] "
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(0, len(result))

    def test_split_json_chunk_with_incomplete_chunk(self):
        chunk = '  [  {"key": "11": "value": "111"} , {"key": "22": "value": "222"} , {"key": "33": "val'
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(3, len(result))
        self.assertEqual('{"key": "11": "value": "111"}', result[0])
        self.assertEqual('{"key": "22": "value": "222"}', result[1])
        self.assertEqual('{"key": "33": "val', result[2])
        with self.assertRaises(json.JSONDecodeError):
            json.loads(result[2])

    def test_split_json_chunk_with_prepend(self):
        prepend = '{"key": "33", "val'
        chunk = 'ue": "333"} , {"key": "44", "value": "444"}  ]  '
        result = tuple(FileDataSource._split_json_chunk(chunk))
        self.assertEqual(2, len(result))
        self.assertEqual('ue": "333"}', result[0])
        with self.assertRaises(json.JSONDecodeError):
            json.loads(result[0])
        self.assertEqual('{"key": "44", "value": "444"}', result[1])

        result = tuple(FileDataSource._split_json_chunk(chunk, prepend))
        self.assertEqual(2, len(result))
        self.assertEqual('{"key": "33", "value": "333"}', result[0])
        self.assertEqual('{"key": "44", "value": "444"}', result[1])
