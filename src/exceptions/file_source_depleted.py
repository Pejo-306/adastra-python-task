class FileSourceDepleted(Exception):

    def __init__(self, filepath: str, message: str = "File source is depleted"):
        self.filepath = filepath
        self.message = message
        super().__init__(message)

    def __str__(self):
        return f"{self.filepath}: {self.message}"
