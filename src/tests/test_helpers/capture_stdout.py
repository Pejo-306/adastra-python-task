import sys
from io import StringIO


class CaptureSTDOUT(list):
    """Container of captured output, written to STDOUT

    The captured output is provided as a standard Python list whose elements are
    separate lines of the output (separated by newline('\n') characters). This
    class is easily utilized via the 'with' syntax in Python.
    NOTE: for more details see https://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call

    Methods:
        __enter__(): redirects the system's output to a temporary string stream
        __exit__(): split the captured string output into a list
    """

    def __enter__(self) -> list:
        """Redirect the system's output to a temporary string stream

        :return: a list of string lines from the captured output
        :rtype: list
        """

        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        """Add the captured lines of output to a list (self)

        Furthermore, redirect the system's output back to STDOUT.

        :param args: not used
        :type args: list
        """

        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout
