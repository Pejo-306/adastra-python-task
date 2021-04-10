import string
from unittest import TestCase
from datetime import datetime

import pytz

from src.sources.data_source import DataSource
from src.sources.simulation_data_source import SimulationDataSource


class TestSimulationDataSource(TestCase):

    def setUp(self):
        self.source = SimulationDataSource()
        self.test_cases = 1

    def test_object_creation(self):
        self.assertIsInstance(self.source, SimulationDataSource)
        self.assertIsInstance(self.source, DataSource)

    def test_has_message(self):
        self.assertTrue(self.source.has_message())

    def test_read(self):
        message = self.source.read()
        self.assertIn("key", message)
        self.assertIn("value", message)
        self.assertIn("ts", message)

    def test_get_random_key(self):
        for _ in range(self.test_cases):
            key = SimulationDataSource._get_random_key()
            self.assertIn(key[0], string.ascii_uppercase)
            self.assertIn(int(key[1:]), range(100, 1000))

    def test_get_random_value(self):
        minimum = 15.0
        maximum = 25.0
        for _ in range(self.test_cases):
            value = SimulationDataSource._get_random_value(minimum, maximum)
            try:
                value = float(value)
                self.assertTrue(minimum <= value <= maximum)
            except ValueError:
                self.fail("Random value cannot be converted to float value")

    def test_get_random_timestamp(self):
        # Test with default datetime range
        earliest = SimulationDataSource.EARLIEST_DATETIME
        latest = SimulationDataSource.LATEST_DATETIME
        for _ in range(self.test_cases):
            ts = datetime.strptime(SimulationDataSource._get_random_timestamp(),
                                   "%Y-%m-%d %H:%M:%S.%f%z")
            self.assertTrue(earliest <= ts <= latest, msg=f"ts = {ts}")

        # Test with custom datetime range
        earliest = datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0, tzinfo=pytz.UTC)
        latest = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0, tzinfo=pytz.UTC)
        for _ in range(self.test_cases):
            ts = datetime.strptime(SimulationDataSource._get_random_timestamp(earliest, latest),
                                   "%Y-%m-%d %H:%M:%S.%f%z")
            self.assertTrue(earliest <= ts <= latest, msg=f"ts = {ts}")
