class FileNotOpenError(Exception):
    """Exception raised when attempting to read from an unopened file

    Attributes:
        filepath(str): path to unopened file
        message(str): error message
    """

    def __init__(self, filepath: str, message: str = "File is not open"):
        """Construct FileNotOpenError

        :param filepath: path to unopened file
        :type filepath: str
        :param message: error message
        :type message: str
        """
        self.filepath = filepath
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        """Get the informal string representation of the error

        :return: full error message
        :rtype: str
        """
        return f"{self.filepath}: {self.message}"
