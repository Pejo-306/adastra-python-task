from unittest import TestCase

from src.exceptions.file_not_open_error import FileNotOpenError


class TestFileNotOpenError(TestCase):

    def test_raise_error(self):
        filepath = "/file/path"
        expected_full_message = f"{filepath}: File is not open"
        with self.assertRaises(FileNotOpenError) as context:
            raise FileNotOpenError(filepath)
        self.assertEqual(filepath, context.exception.filepath)
        self.assertEqual("File is not open", context.exception.message)
        self.assertEqual(expected_full_message, str(context.exception))
