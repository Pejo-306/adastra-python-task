import string
import random
from datetime import datetime

from src.sources.data_source import DataSource


class SimulationDataSource(DataSource):
    """Data source which generates random messages when queried

    This data source has an infinite amount of messages with random data. Messages
    are generated on a one by one basis when the data source is asked with the
    following format:
        `{"key": <value>, "value": <value>, "ts": <value>}`

    Methods:
        __enter__(): (see DataSource)
        __exit__(): (see DataSource)
        initialize(): do nothing (see DataSource)
        has_message(): indicate that there is an available message for extraction
        read(): generate and return a message with random data
        close(): do nothing (see DataSource)

    Static methods:
        _get_random_key(): generate a random message key
        _get_random_value(minimum, maximum): generate a random message value in the
                                             range [minimum, maximum]
        _get_random_timestamp(earliest, latest): generate a random timestamp in the
                                                 range [earliest, latest]
    """

    LATEST_DATETIME = datetime(year=2020, month=12, day=31, hour=23, minute=59, second=59)

    def __enter__(self):
        """Do nothing (see DataSource and SimulationDataSource.initialize())"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Do nothing (see DataSource and SimulationDataSource.close())"""
        pass

    def initialize(self) -> None:
        """Do nothing (see DataSource)

        Simulation data sources do not require any preemptive setup.
        """
        pass

    def has_message(self) -> bool:
        """Indicate that there is an available message for extraction

        Since this data source is infinite, this method always returns True.

        :return: True
        :rtype: bool
        """
        return True

    def read(self) -> dict:
        """Generate and return a message with random data

        :return: body of message with random data
        :rtype: dict
        """
        return {
            "key": self._get_random_key(),
            "value": self._get_random_value(),
            "ts": self._get_random_timestamp()
        }

    def close(self) -> None:
        """Do nothing (see DataSource)

        Simulation data sources do not require any cleanup/termination.
        """
        pass

    @staticmethod
    def _get_random_key() -> str:
        """Generate a random message key

        Generated keys are in the format: "<L><NNN>", where "L" is a random uppercase
        letter and "NNN" is a random 3-digit number.
        Examples: "A123", "B100", "Z999".

        :return: random message key
        :rtype: str
        """
        return random.choice(string.ascii_uppercase) + str(random.randint(100, 999))

    @staticmethod
    def _get_random_value(minimum: float = 0.0, maximum: float = 100.0) -> str:
        """Generate a random message value

        Message values are floats in the range [minimum, maximum].

        :param minimum: minimum value, inclusive
        :type minimum: float
        :param maximum: maximum value, inclusive
        :type maximum: float

        :return: random message value
        :rtype: str
        """
        return str(random.uniform(minimum, maximum))

    @staticmethod
    def _get_random_timestamp(earliest: datetime = datetime.min,
                              latest: datetime = LATEST_DATETIME) -> str:
        """Generate a random message timestamp

        Message timestamps are randomly selected from the range [earliest, latest].

        :param earliest: minimum/earliest datetime, inclusive
        :type earliest: datetime
        :param latest: maximum/latest datetime, inclusive
        :type latest: datetime

        :return: random message timestamp
        :rtype: str
        """
        # TODO: add timezone info
        delta = latest - earliest
        return str(earliest + delta * random.uniform(0.0, 1.0))
