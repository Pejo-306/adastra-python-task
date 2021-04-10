import psycopg2
import psycopg2.errors
from unittest import TestCase
from datetime import datetime

from src.sinks.data_sink import DataSink
from src.sinks.postgresql_data_sink import PostgreSQLDataSink


class TestPostgreSQLDataSink(TestCase):

    def setUp(self):
        self.dbname = "test_schema"
        self.dbuser = "user"
        self.dbpassword = "user"
        self.dbhost = "127.0.0.1"
        self.dbport = 5432
        self.sink = PostgreSQLDataSink(self.dbname, self.dbuser, self.dbpassword, self.dbhost, self.dbport)
        try:
            self.con = psycopg2.connect(database=self.dbname, user=self.dbuser, password=self.dbpassword,
                                        host=self.dbhost, port=self.dbport)
        except psycopg2.OperationalError:
            self.fail(f"Test database with name '{self.dbname}' does not exist")

    def tearDown(self):
        try:
            with self.con.cursor() as cur:
                cur.execute('DELETE FROM "Message";')  # delete all dumped messages
                self.con.commit()
        except psycopg2.errors.UndefinedTable:
            pass  # table has not been created yet
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
        # Make sure there are no messages before dumping
        with self.con.cursor() as cur:
            cur.execute('SELECT * FROM "Message";')
            messages = cur.fetchall()
        self.assertEqual(0, len(messages))

        message = {
            "key": "A123",
            "value": "15.6",
            "ts": "2020-10-07 13:28:43.399620+02:00"
        }
        self.sink.dump(message)
        with self.con.cursor() as cur:
            cur.execute('SELECT * FROM "Message";')
            messages = cur.fetchall()
        self.assertEqual(1, len(messages))
        self.assertEqual("A123", messages[0][0])  # key
        self.assertEqual("15.6", messages[0][1])  # value
        self.assertEqual("2020-10-07 13:28:43.399620+02:00", messages[0][2])  # timestamp

    def test_multiple_dumps(self):
        # Make sure there are no messages before dumping
        with self.con.cursor() as cur:
            cur.execute('SELECT * FROM "Message";')
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
        self.sink.dump(message1)
        self.sink.dump(message2)
        with self.con.cursor() as cur:
            cur.execute('SELECT * FROM "Message";')
            messages = cur.fetchall()
        self.assertEqual(2, len(messages))
        self.assertEqual("A123", messages[0][0])  # key
        self.assertEqual("15.6", messages[0][1])  # value
        self.assertEqual("2020-10-07 13:28:43.399620+02:00", messages[0][2])  # timestamp
        self.assertEqual("B123", messages[1][0])  # key
        self.assertEqual("12.6", messages[1][1])  # value
        self.assertEqual("2022-10-07 13:28:43.399620+02:00", messages[1][2])  # timestamp
