"""
    dr_utils.py contains generic functions used in multiple ETLs
"""

import yaml
import logging
import pandas as pd
import sqlite3
from os import path

log = logging.getLogger(name=__name__)


def get_config_parameters(config_file, top_level_key):
    """
    Gets set of keys and values from config file

    :param config_file: YAML config file (str)
    :param top_level_key: top level key to retrieve (str)
    :return: sub keys and values (dict)
    """
    with open(config_file, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
        try:
            return config[top_level_key]
        except KeyError:
            raise KeyError(
                f'Top level key "{top_level_key}" is not available in the configuration file {config_file}'
            )


# IMPROVEMENTS
# This function does a 'quick and dirty' load leaving to pandas to decide field types. If implemented in a real-world
# scenario tables would be persistent, optimised for storage and querying depending on the technology used (indexing,
# sorting, partitioning, etc.) and ideally check that the number of errors loaded is the expected based on dataframe
# size.

def load_csv_to_db(csv_file, header, db_file, table_name):
    """
    Loads CSV file in SQLite database

    :param csv_file: input csv file to load (str)
    :param header: columns names to use when not available in input file, if none the first row will be used (str)
    :param db_file: SQLite database file where data will be loaded (str)
    :param table_name: talbe where csv file will be loaded (str)
    :return: none
    """
    conn = sqlite3.connect(db_file)
    df = pd.read_csv(csv_file)
    if header:
        df.columns = header
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log.info(f'File {csv_file} loaded into table {table_name}')


# IMPROVEMENTS
# In a real world scenarion this would be done by a infraestructure deployment process and never by the ETL. This
# solution was adopted for portability purposes.

def create_db(db_name, output_folder):
    """
    Creates SQLite database file

    :param db_name: database name (str)
    :param output_folder: folder where the database will be created (str)
    :return: full path to the output file (str)
    """
    out_file = path.join(output_folder, db_name + '.db')
    sqlite3.connect(out_file)
    log.info(f'SQLite database {db_name}.db created in {output_folder}')
    return out_file
