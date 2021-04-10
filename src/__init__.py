import os

from src.definitions import ROOT_DIR, DATABASE_ENV

# Preload values from "database.env" into a global dictionary
with open(os.path.join(ROOT_DIR, "database.env")) as dbenv:
    for parameter in dbenv:
        parameter_name, parameter_value = parameter.strip().split('=')
        DATABASE_ENV[parameter_name] = parameter_value
