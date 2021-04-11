import os
from typing import Tuple

from src.definitions import INPUT_FILES_DIR, DATABASE_ENV
from src.etl import ETL
from src.sources.data_source import DataSource
from src.sources.file_data_source import FileDataSource
from src.sources.simulation_data_source import SimulationDataSource
from src.sinks.data_sink import DataSink
from src.sinks.console_data_sink import ConsoleDataSink
from src.sinks.postgresql_data_sink import PostgreSQLDataSink


def main() -> None:
    quit_ = False
    while not quit_:
        clear()
        print("1. Start ETL")
        print("2. Exit")
        print(">> ", end='')
        option = input()
        if option == '1':
            source_cls, source_args = select_source()
            sink_cls, sink_args = select_sink()
            print("Launching ETL process...")
            ETL().source(source_cls, *source_args).sink(sink_cls, *sink_args).run()
            print("ETL process has finished. Press 'Enter' to continue...")
            input()
        elif option == '2':
            print("Quiting...")
            quit_ = True
        else:
            print("Invalid input. Press 'Enter' to continue...")
            input()


def select_source() -> Tuple[DataSource, list]:
    source_cls, source_args = None, []
    valid = False
    while not valid:
        clear()
        print("Select a data source class:")
        print("1: Simulation")
        print("2: File")
        print(">> ", end='')
        option = input()
        if option == '1':
            source_cls = SimulationDataSource
            valid = True
        elif option == '2':
            source_cls = FileDataSource
            print("'source filepath':")
            print(f">> {INPUT_FILES_DIR}/", end='')
            source_args.append(os.path.join(INPUT_FILES_DIR, input()))
            print("'chunk size': (default is 256)")
            print(">> ", end='')
            chunk_size = input()
            if len(chunk_size.strip()):  # user has entered custom chunk size
                source_args.append(int(chunk_size))
            valid = True
        else:
            print("Invalid input. Press 'Enter' to continue...")
            input()
    return source_cls, source_args


def select_sink() -> Tuple[DataSink, list]:
    sink_cls, sink_args = None, []
    valid = False
    while not valid:
        clear()
        print("Select a data sink class:")
        print("1: Console")
        print("2: PostgreSQL")
        print(">> ", end='')
        option = input()
        if option == '1':
            sink_cls = ConsoleDataSink
            print("'output format': (default is 'key: {} | value: {} | ts: {}')")
            print(">> ", end='')
            output_format = input()
            sink_args.append(output_format if len(output_format.strip()) else "key: {} | value: {} | ts: {}")
            valid = True
        elif option == '2':
            sink_cls = PostgreSQLDataSink
            print(f"'DB name': (default is '{DATABASE_ENV['POSTGRES_DB']}')")
            print(f">> ", end='')
            dbname = input()
            sink_args.append(dbname.strip() if len(dbname.strip()) else DATABASE_ENV["POSTGRES_DB"])
            print(f"'DB user': (default is '{DATABASE_ENV['POSTGRES_USER']}')")
            print(f">> ", end='')
            dbuser = input()
            sink_args.append(dbuser.strip() if len(dbuser.strip()) else DATABASE_ENV["POSTGRES_USER"])
            print(f"'DB password': (default is '{DATABASE_ENV['POSTGRES_PASSWORD']}')")
            print(f">> ", end='')
            dbpassword = input()
            sink_args.append(dbpassword.strip() if len(dbpassword.strip()) else DATABASE_ENV["POSTGRES_PASSWORD"])
            print("'DB host': (default is '127.0.0.1')")
            print(f">> ", end='')
            dbhost = input()
            if len(dbhost.strip()):  # user has entered custom DB host
                sink_args.append(dbhost)
            print("'DB port': (default is '5432')")
            print(f">> ", end='')
            dbport = input()
            if len(dbport.strip()):  # user has entered custom DB port
                sink_args.append(int(dbport))
            valid = True
        else:
            print("Invalid input. Press 'Enter' to continue...")
            input()
    return sink_cls, sink_args


def clear() -> None:
    os.system("cls" if os.name == "nt" else "clear")


if __name__ == "__main__":
    main()
