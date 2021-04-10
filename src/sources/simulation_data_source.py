import string
import random
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
        return True

    def read(self) -> dict:
        return {
            "key": self._get_random_key(),
            "value": self._get_random_value(),
            "ts": self._get_random_timestamp()
        }

    def close(self) -> None:
        pass

    @staticmethod
    def _get_random_key() -> str:
        return random.choice(string.ascii_uppercase) + str(random.randint(100, 999))

    @staticmethod
    def _get_random_value(minimum: float = 0.0, maximum: float = 100.0) -> str:
        return str(random.uniform(minimum, maximum))

    @staticmethod
    def _get_random_timestamp(earliest: datetime = datetime.min,
                              latest: datetime = LATEST_DATETIME) -> str:
        delta = latest - earliest
        return str(earliest + delta * random.uniform(0.0, 1.0))
