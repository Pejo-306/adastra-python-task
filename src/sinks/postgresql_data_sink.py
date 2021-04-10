import psycopg2

from src.sinks.data_sink import DataSink


class PostgreSQLDataSink(DataSink):

    def __init__(self, dbname: str, dbuser: str, dbpassword: str,
                 dbhost: str = "127.0.0.1", dbport: int = 5432):
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbhost = dbhost
        self.dbport = dbport
        self._con = None

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def initialize(self) -> None:
        # First, establish a connection to the specified database
        try:
            self._connect_to_db()
        except psycopg2.OperationalError:  # specified database does not exist
            with psycopg2.connect(database="default_schema", user=self.dbuser,
                                  password=self.dbpassword, host=self.dbhost,
                                  port=str(self.dbport)) as con:
                with con.cursor() as cur:
                    con.autocommit = True  # cannot create db inside a transaction
                    cur.execute(f'CREATE DATABASE "{self.dbname}"')
                    con.autocommit = False
            self._connect_to_db()  # try again

        # TODO: Second, create the necessary database table, if required

    def dump(self, message: dict) -> bool:
        pass

    def close(self) -> None:
        self._con.close()

    def _connect_to_db(self) -> None:
        self._con = psycopg2.connect(database=self.dbname, user=self.dbuser,
                                     password=self.dbpassword, host=self.dbhost,
                                     port=str(self.dbport))
