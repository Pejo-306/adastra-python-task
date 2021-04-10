from src.sinks.data_sink import DataSink


class PostgreSQLDataSink(DataSink):

    def __init__(self, dbname: str, dbuser: str, dbpassword: str,
                 dbhost: str = "127.0.0.1", dbport: int = 5432):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def initialize(self) -> None:
        pass

    def dump(self, message: dict) -> bool:
        pass

    def close(self) -> None:
        pass
