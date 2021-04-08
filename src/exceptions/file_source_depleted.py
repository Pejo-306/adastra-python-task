class FileSourceDepleted(Exception):
    """Exception raised when attempting to read from a depleted source file

    Attributes:
        filepath(str): path to depleted file
        message(str): error message
    """

    def __init__(self, filepath: str, message: str = "File source is depleted"):
        """Construct FileSourceDepleted

        :param filepath: path to depleted file
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
