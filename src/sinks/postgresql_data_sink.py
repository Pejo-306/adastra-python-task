import re

import psycopg2

from src.definitions import DATABASE_ENV
from src.sinks.data_sink import DataSink


class PostgreSQLDataSink(DataSink):
    """Data sink which dumps its messages into a PostgreSQL database

    The datasource manages a connection to a specified PostgreSQL database which
    must already be set up and running. Incoming messages must adhere to the
    following format before they are stored in the database table:
        `{"key": "<key>", "value": "<value>", "ts": "<timestamp>+<timezone>"}`

    If the specified database does not exist and therefore the data sink cannot
    connect to PostgreSQL, the sink will attempt to connect to the default database
    and create a new database from there. Afterwards, it will relog in the new database.

    Class attributes:
        MESSAGE_TABLE_NAME(str): name of database table where messages are dumped
        TIMESTAMP_PATTERN(re.Match): compiled regex object for timestamps with timezone info

    Attributes:
        dbname(str): database name
        dbuser(str): valid PostgreSQL database user
        dbpassword(str): password for valid PostgreSQL database user
        dbhost(str): ip address of PostgreSQL host (default is "localhost")
        dbport(int): port on which PostgreSQL is running (default is "5432")
        _connection(psycopg2.extensions.connection): established and active connection
                                                     to the PostgreSQL database

    Methods:
        __enter__(): (see DataSink)
        __exit__(): (see DataSink)
        initialize(): connect to and setup the database
        dump(message): save the message as a row in the database message table
        close(): terminate the connection to the database
        _connect_to_db(): establish a connection to the database
    """

    MESSAGE_TABLE_NAME = "Message"
    TIMESTAMP_PATTERN = re.compile(r"^(?P<ts>[-.:0-9 ]+)(?P<tz>[+-][0-9:]+)$")

    def __init__(self, dbname: str, dbuser: str, dbpassword: str,
                 dbhost: str = "127.0.0.1", dbport: int = 5432):
        """Construct PostgreSQL data sink

        :param dbname: database name
        :type dbname: str
        :param dbuser: valid PostgreSQL database user
        :type dbuser: str
        :param dbpassword: password for valid PostgreSQL database user
        :type dbpassword: str
        :param dbhost: ip address of PostgreSQL host (default is "localhost")
        :type dbhost: str
        :param dbport: port on which PostgreSQL is running (default is "5432")
        :type dbport: int
        """
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbhost = dbhost
        self.dbport = dbport
        self._connection = None

    def __enter__(self):
        """Ensure proper initialization of PostgreSQL data sink"""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper termination of PostgreSQL data sink"""
        self.close()

    def initialize(self) -> None:
        """Connect to and setup the database

        Initially, the data sink attempts to connect to the specified database.
        If the operation is unsuccessful, the sink connects to the default database
        and creates a new database, after which the sink connects to the latter.

        If the necessary database message table does not exist, it is created.
        Otherwise, no additional setup is required.
        """
        # First, establish a connection to the specified database
        try:
            self._connect_to_db()
        except psycopg2.OperationalError:  # specified database does not exist
            with psycopg2.connect(database=DATABASE_ENV["POSTGRES_DB"],
                                  user=self.dbuser, password=self.dbpassword,
                                  host=self.dbhost, port=str(self.dbport)) as con:
                with con.cursor() as cur:
                    con.autocommit = True  # cannot create db inside a transaction
                    cur.execute(f'CREATE DATABASE "{self.dbname}"')
                    con.autocommit = False
            self._connect_to_db()  # try again

        # Second, create the necessary database table, only if required
        with self._connection.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.MESSAGE_TABLE_NAME}" (
                    id SERIAL PRIMARY KEY,
                    key CHAR(4) NOT NULL,
                    value REAL NOT NULL,
                    ts TIMESTAMP NOT NULL,
                    tz TEXT NOT NULL
                );
            """)
            self._connection.commit()

    def dump(self, message: dict) -> bool:
        """Save the message as a row in the database message table

        Note that the message's timestamp must contain timezone info.

        :param message: body of the message
        :type message: dict

        :return: status which indicates whether the dump was successful
        :rtype: bool
        """
        match = self.TIMESTAMP_PATTERN.match(message["ts"])
        if not match:  # improperly formatted timestamp
            raise ValueError(f'Improperly formatted timestamp: {message["ts"]}')
        ts, tz = match.group("ts"), match.group("tz")
        with self._connection.cursor() as cur:
            cur.execute(f"""
                INSERT INTO "{self.MESSAGE_TABLE_NAME}" (key, value, ts, tz) 
                VALUES ('{message["key"]}', {message["value"]}, '{ts}', '{tz}');
            """)
            self._connection.commit()
        return True  # since no errors are raised by psycopg2, dump is successful

    def close(self) -> None:
        """Terminate the connection to the database"""
        self._connection.close()

    def _connect_to_db(self) -> None:
        """Establish a connection to the PostgreSQL database"""
        self._connection = psycopg2.connect(database=self.dbname,
                                            user=self.dbuser, password=self.dbpassword,
                                            host=self.dbhost, port=str(self.dbport))
