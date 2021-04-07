import os
from unittest import TestCase

from src.definitions import INPUT_FILES_DIR
from src.sources.data_source import DataSource
from src.sources.file_data_source import FileDataSource


class TestFileDataSource(TestCase):

    def test_object_creation(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        self.assertIsInstance(source, FileDataSource)
        self.assertIsInstance(source, DataSource)
        self.assertEquals(source.source_filepath, source_filepath)

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
            self.assertEquals(message["key"], "A123")
            self.assertIn("value", message)
            self.assertEquals(message["value"], "15.6")
            self.assertIn("ts", message)
            self.assertEquals(message["ts"], "2020-10-07 13:28:43.399620+02:00")
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_multiple_reads(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "multiple_messages.json")
        source = FileDataSource(source_filepath)
        try:
            source.initialize()

            message = source.read()  # first message
            self.assertIn("key", message)
            self.assertEquals(message["key"], "A123")
            self.assertIn("value", message)
            self.assertEquals(message["value"], "15.6")
            self.assertIn("ts", message)
            self.assertEquals(message["ts"], "2020-10-07 13:28:43.399620+02:00")

            message = source.read()  # second message
            self.assertIn("key", message)
            self.assertEquals(message["key"], "B123")
            self.assertIn("value", message)
            self.assertEquals(message["value"], "12.6")
            self.assertIn("ts", message)
            self.assertEquals(message["ts"], "2022-10-07 13:28:43.399620+02:00")
        finally:
            source.close()  # assumes FileDataSource.close() works

    def test_close(self):
        source_filepath = os.path.join(INPUT_FILES_DIR, "single_message.json")
        source = FileDataSource(source_filepath)
        source.initialize()
        self.assertTrue(source.is_open)
        source.close()
        self.assertFalse(source.is_open)
