from datetime import datetime

from src.sources.data_source import DataSource


class SimulationDataSource(DataSource):

    LATEST_DATETIME = datetime(year=2020, month=12, day=31, hour=23, minute=59, second=59)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def initialize(self) -> None:
        pass

    def has_message(self) -> bool:
        pass

    def read(self) -> dict:
        pass

    def close(self) -> None:
        pass

    @staticmethod
    def _get_random_key() -> str:
        pass

    @staticmethod
    def _get_random_value() -> str:
        pass

    @staticmethod
    def _get_random_timestamp(earliest: datetime = datetime.min,
                              latest: datetime = LATEST_DATETIME) -> str:
        pass
