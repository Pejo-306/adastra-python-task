# Lightweight ETL System

Pseudo ETL system in *Python* with a simple interface for data sources and data sinks.
It was developed as a small task when I was applying to for a junior position in
**Adastra Bulgaria**.

## Table of contents

* [Short description](#short-description)
* [Getting started](#getting-started)
  - [TL;DR](#tldr)
  - [Prerequisites](#prerequisites)
  - [Getting the project](#getting-the-project)
  - [Setting up a Python virtual environment](#setting-up-a-python-virtual-environment)
  - [Running a PostgreSQL server with Adminer](#running-a-postgresql-server-with-adminer)
  - [Running the pseudo ETL system](#running-the-pseudo-etl-system)
  - [Running Python unit tests (Optional)](#running-python-unit-tests-optional)
* [Built with](#built-with)
* [License](#license)
* [See more](#see-more)

## Short description

The project is a simple pseudo ETL system which extracts messages from a data source
and dumps them in a data sink on a one by one basis. The system does not perform any
kind of aggregation, manipulation, etc. of the received data.

The ETL system extracts data from the following data sources:
* **Simulation**: generates random data when queried.
* **File**: reads messages from a JSON file which contains a JSON array of messages.

Furthermore, the ETL system dumps its data in the following data sinks:
* **Console**: messages are printed to *STDOUT*.
* **PostgreSQL**: messages are inserted into a database table in *PostgreSQL*.

Messages which are processed by the ETL system are short JSON objects which have
three attributes: **'key'** - a short string, **'value'** - decimal value,
**'ts'** - timestamp with timezone info attached at the end. All messages must be 
formatted as such: 
```JSON
{"key": "<value>", "value": "<value>", "ts": "<value>"}
```
Example:
```JSON
{"key": "A123", "value": "15.6", "ts": "2020-10-07 13:28:43.399620+02:00"}
```

## Getting started

This section contains instructions on how to download, setup and run the pseudo ETL
system.

### TL;DR

Clone the repo:
```bash
$ git clone https://github.com/Pejo-306/adastra-python-task
$ cd adastra-python-task/
```

(Optional | Recommended) Setup virtual environment (*venv*) and install **pytz** and **psycopg2**:
```bash
$ python3 -m venv ./venv
$ ./venv/bin/python3 -m pip install pytz psycopg2_binary
```

(Optional | Recommended) In another shell window launch [Docker](https://www.docker.com/) containers 
with **PostgreSQL** and **Adminer**:
```bash
$ docker-compose up
```

(Optional | Recommended) Run via virtual environment **Python 3** interpreter:
```bash
$ ./venv/bin/python3 main.py
```

Run via system's **Python 3** interpreter:
```bash
$ python3 main.py
```

### Prerequisites

You must have **Python3.8+** interpreter, as well as the following packages: 
**pytz** and **psycopg2**. This project was built with the following versions:

* Python 3.8.5
* pytz 2021.1
* psycopg2-binary 2.8.6

In [this](#setting-up-a-python-virtual-environment) section it is explained how 
to set up a Python virtual environment and install the necessary packages.

### Getting the project

To get a copy of the project, clone the repository like so:
```bash
$ git clone https://github.com/Pejo-306/adastra-python-task
$ cd adastra-python-task/
```

### Setting up a Python virtual environment

Whilst you can run the project via your system's **Python** interpreter, it is
generally discouraged to install third party libraries and to alter the system's
interpreter's version, as well as the system's packages' versions, to comply with
any project's version requirements. Therefore, it is advised to set up a **Python**
virtual environment with **Python's** built-in module *'venv'*.

To set up a virtual **Python 3** environment, run the following command:
```bash
$ python3 -m venv ./venv
```
which creates a virtual environment in the *'./venv'* directory. A **Python 3**
interpreter is now available in *'./venv/bin/python3'*.

Next, you must install the necessary third party modules, utilized in this project.
The **pytz** library is used to accurately keep track of timezone-aware datetime
objects. The **psycopg2** module is a *PostgreSQL* database adapter, used to
manage and communicate with a *PostgreSQL* database. To install both packages run:
```bash
$ ./venv/bin/python3 -m pip install pytz psycopg2_binary
```

### Running a PostgreSQL server with Adminer

The pseudo ETL system requires a working *PostgreSQL* server when using a
*PostgreSQL* data sink to persist data. If you already have a running *PostgreSQL*
server, you may skip this section.

Alternatively, this project comes with a ['docker-compose.yml'](docker-compose.yml)
file which creates two [Docker](https://www.docker.com/) containers - one with a
*PostgreSQL* server, the other with *Adminer* for managing the former database server.
In order to use this file, you must have both [Docker](https://www.docker.com/) and
[Docker Compose](https://github.com/docker/compose) installed on your machine. At the
time of writing the following versions are used:

* Docker version 20.10.5
* docker-compose version 1.27.4

After you have [Docker](https://www.docker.com/) and 
[Docker Compose](https://github.com/docker/compose) installed, you have to run
the following command*, which creates the wanted docker containers**:

```bash
$ docker-compose up
```

*__Note__: if you already have a *PostgreSQL* server or other service running on port
**5432** (*PostgreSQL's* default port), then [Docker Compose](https://github.com/docker/compose) 
will fail to start its own *PostgreSQL* server. You must first disable said service
to release port **5432**. Another option is to edit the ['docker-compose.yml'](docker-compose.yml)
file to run the *PostgreSQL* server on another port.

**__Note__: the containers are named **'dev_postgres'** and **'dev_adminer'**, respectively
for the *PostgreSQL* server and for the *Adminer* service. If your machine's [Docker](https://www.docker.com/)
already has created other containers with the same names, then [Docker Compose](https://github.com/docker/compose)
will fail to create the necessary containers. In this scenario you must either edit
the ['docker-compose.yml'](docker-compose.yml) file and rename the containers, or
delete your own containers with the conflicting names.

A *PostgreSQL* server will then be running on ***localhost:5432***. You may also
access *Adminer* via a browser by visiting [http://localhost:8080](http://localhost:8080).

Also, it should be noted that the ['docker-compose.yml'](docker-compose.yml) file
uses parameters from ['database.env'](database.env) to set up the *PostgreSQL*
server. You may alter the values of ['database.env'](database.env) as you see
fit, however **DO NOT** delete the file or any of the parameters, since the 
**Python** source code of the project uses it.

### Running the pseudo ETL system

After a **Python** environment is set up and a working *PostgreSQL* server is 
deployed, you may run the project as such (via virtual environment):
```bash
$ ./venv/bin/python3 main.py
```

Alternatively, you may run via the system's **Python 3** interpreter:
```bash
$ python3 main.py
```

A simple console front-end is provided to work with ETL tasks.

### Running Python unit tests (Optional)

You may run this project's **Python** unit tests with the following command
(from the project's root directory)*:
```bash
$ ./venv/bin/python3 -m unittest discover -s ./src/tests/
```

Alternatively, via the system's **Python 3** interpreter:
```bash
$ python3 -m unittest discover -s ./src/tests/
```

*__Note__: you must have a running *PostgreSQL* server, otherwise the unit tests for
*'PostgreSQLDataSink'* will fail. See [this section](#running-a-postgresql-server-with-adminer)
for more details on how to set up a *PostgreSQL* server.

## Built with

* [Python 3](https://www.python.org/)
  - [pytz](https://pypi.org/project/pytz/)
  - [psycopg2](https://pypi.org/project/psycopg2/)
* [Docker](https://www.docker.com/) and [Docker Compose](https://github.com/docker/compose)

## License

This project is distributed under the [MIT license](LICENSE).

## See more

* [Simple Pseudo ETL System - Dockerized](https://github.com/Pejo-306/adastra-python-task-dockerized):
  This ETL system funneled from local application to a Docker Swarm stack via a CI/CD pipeline
