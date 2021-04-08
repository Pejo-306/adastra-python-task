from unittest import TestCase

from src.exceptions.file_not_open_error import FileNotOpenError


class TestFileNotOpenError(TestCase):

    def test_raise_error(self):
        filepath = "/file/path"
        expected_full_message = "{}: File is not open".format(filepath)
        try:
            raise FileNotOpenError(filepath)
        except FileNotOpenError as e:
            self.assertEqual(filepath, e.filepath)
            self.assertEqual("File is not open", e.message)
            self.assertEqual(expected_full_message, str(e))
