from unittest import TestCase
from datetime import datetime

import psycopg2
import psycopg2.errors

from src.definitions import DATABASE_ENV
from src.sinks.data_sink import DataSink
from src.sinks.postgresql_data_sink import PostgreSQLDataSink


class TestPostgreSQLDataSink(TestCase):

    def setUp(self):
        self.dbname = "test_schema"
        self.dbuser = DATABASE_ENV["POSTGRES_USER"]
        self.dbpassword = DATABASE_ENV["POSTGRES_PASSWORD"]
        self.dbhost = "127.0.0.1"
        self.dbport = 5432
        self.sink = PostgreSQLDataSink(self.dbname, self.dbuser, self.dbpassword, self.dbhost, self.dbport)
        try:
            self.con = psycopg2.connect(database=self.dbname, user=self.dbuser, password=self.dbpassword,
                                        host=self.dbhost, port=self.dbport)
        except psycopg2.OperationalError:
            self.fail(f"Test database with name '{self.dbname}' does not exist")

    def tearDown(self):
        self.con.close()

    def test_object_creation(self):
        self.assertIsInstance(self.sink, PostgreSQLDataSink)
        self.assertIsInstance(self.sink, DataSink)
        self.assertEqual(self.dbname, self.sink.dbname)
        self.assertEqual(self.dbuser, self.sink.dbuser)
        self.assertEqual(self.dbpassword, self.sink.dbpassword)
        self.assertEqual(self.dbhost, self.sink.dbhost)
        self.assertEqual(self.dbport, self.sink.dbport)

    def test_initialize_with_new_database(self):
        new_dbname = f"__new_test_schema_{datetime.now()}"  # ensure uniqueness of database name
        with self.assertRaises(psycopg2.OperationalError):  # unique database should not exist
            psycopg2.connect(database=new_dbname, user=self.dbuser, password=self.dbpassword,
                             host=self.dbhost, port=self.dbport)
        self.sink.dbname = new_dbname
        try:
            self.sink.initialize()
            con = psycopg2.connect(database=new_dbname, user=self.dbuser, password=self.dbpassword,
                                   host=self.dbhost, port=self.dbport)
        except psycopg2.OperationalError:
            self.fail(f"New database with unique name '{new_dbname}' was not created")
        else:
            # close connection to currently open database
            con.close()
            self.sink.close()
            # drop the new database from the test connection
            with self.con.cursor() as cur:
                self.con.autocommit = True  # cannot drop db from within transaction
                cur.execute(f'DROP DATABASE "{new_dbname}"')  # clean up created database
                self.con.autocommit = False

    def test_dump(self):
        self.sink.initialize()
        # Make sure there are no messages before dumping
        with self.con.cursor() as cur:
            cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
            messages = cur.fetchall()
        self.assertEqual(0, len(messages))

        message = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        try:
            success = self.sink.dump(message)
            self.assertTrue(success, "Dump was not successful")
            self.sink.close()
            with self.con.cursor() as cur:
                cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
                messages = cur.fetchall()
            self.assertEqual(1, len(messages))
            self.assertEqual("A123", messages[0][1])  # key
            self.assertEqual(15.6, messages[0][2])  # value
            self.assertEqual("2020-10-07 13:28:43.399620+02:00", f"{messages[0][3]}{messages[0][4]}")  # timestamp
        finally:  # Clean-up
            with self.con.cursor() as cur:
                cur.execute(f'DELETE FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')  # delete all dumped messages
                self.con.commit()

    def test_multiple_dumps(self):
        # Make sure there are no messages before dumping
        self.sink.initialize()
        with self.con.cursor() as cur:
            cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
            messages = cur.fetchall()
        self.assertEqual(0, len(messages))

        message1 = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        message2 = {
            "key": "B123",
            "value": "12.6",
            "ts": "2022-10-07 13:28:43.399620+02:00"
        }
        try:
            success = self.sink.dump(message1)
            self.assertTrue(success, "Dump was not successful")
            success = self.sink.dump(message2)
            self.assertTrue(success, "Dump was not successful")
            self.sink.close()
            with self.con.cursor() as cur:
                cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
                messages = cur.fetchall()
            self.assertEqual(2, len(messages))
            self.assertEqual("A123", messages[0][1])  # key
            self.assertEqual(15.6, messages[0][2])  # value
            self.assertEqual("2020-10-07 13:28:43.399620+02:00", f"{messages[0][3]}{messages[0][4]}")  # timestamp
            self.assertEqual("B123", messages[1][1])  # key
            self.assertEqual(12.6, messages[1][2])  # value
            self.assertEqual("2022-10-07 13:28:43.399620+02:00", f"{messages[1][3]}{messages[1][4]}")  # timestamp
        finally:  # Clean-up
            with self.con.cursor() as cur:
                cur.execute(f'DELETE FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')  # delete all dumped messages
                self.con.commit()

    def test_dump_with_invalid_timestamp(self):
        self.sink.initialize()
        # Make sure there are no messages before dumping
        with self.con.cursor() as cur:
            cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
            messages = cur.fetchall()
        self.assertEqual(0, len(messages))

        message = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620"
        }
        try:
            with self.assertRaises(ValueError):
                self.sink.dump(message)
            self.sink.close()
            with self.con.cursor() as cur:
                cur.execute(f'SELECT * FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')
                messages = cur.fetchall()
            self.assertEqual(0, len(messages))
        finally:  # Clean-up
            with self.con.cursor() as cur:
                cur.execute(f'DELETE FROM "{PostgreSQLDataSink.MESSAGE_TABLE_NAME}";')  # delete all dumped messages
                self.con.commit()
