from unittest import TestCase
from datetime import datetime

from src.sources.data_source import DataSource
from src.sources.simulation_data_source import SimulationDataSource


class TestSimulationDataSource(TestCase):

    def setUp(self):
        self.source = SimulationDataSource()

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
        test_cases = 100
        for _ in range(test_cases):
            key = SimulationDataSource._get_random_key()
            self.assertIn(key[0], range(ord('A'), ord('Z') + 1))
            self.assertIn(int(key[1:]), range(100, 1000))

    def test_get_random_value(self):
        test_cases = 100
        for _ in range(test_cases):
            value = SimulationDataSource._get_random_value()
            try:
                float(value)
            except ValueError:
                self.fail("Random value cannot be converted to float value")

    def test_get_random_timestamp(self):
        test_cases = 100

        # Test with default datetime range
        for _ in range(test_cases):
            ts = datetime.fromisoformat(SimulationDataSource._get_random_timestamp())
            self.assertTrue(datetime.min <= ts <= SimulationDataSource.LATEST_DATETIME)

        # Test with custom datetime range
        earliest = datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0)
        latest = datetime(year=2020, month=1, day=1, hour=0, minute=0, second=0)
        for _ in range(test_cases):
            ts = datetime.fromisoformat(SimulationDataSource._get_random_timestamp(earliest, latest))
            self.assertTrue(earliest <= ts <= latest)
