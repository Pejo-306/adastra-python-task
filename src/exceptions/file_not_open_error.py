class FileNotOpenError(Exception):

    def __init__(self, filepath: str, message: str = "File is not open"):
        self.filepath = filepath
        self.message = message
        super().__init__(message)

    def __str__(self):
        return f"{self.filepath}: {self.message}"
