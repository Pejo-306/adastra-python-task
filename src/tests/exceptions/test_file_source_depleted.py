from unittest import TestCase

from src.exceptions.file_source_depleted import FileSourceDepleted


class TestFileSourceDepleted(TestCase):

    def test_raise_error(self):
        filepath = "/file/path"
        expected_full_message = f"{filepath}: File source is depleted"
        with self.assertRaises(FileSourceDepleted) as context:
            raise FileSourceDepleted(filepath)
        self.assertEqual(filepath, context.exception.filepath)
        self.assertEqual("File source is depleted", context.exception.message)
        self.assertEqual(expected_full_message, str(context.exception))
