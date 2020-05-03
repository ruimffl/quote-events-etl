"""
 dr_utils.py contains generic Digital Risks functions used in multiple ETLs
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


def load_csv_to_db(csv_file, header, db_file, table_name):
    """

    :param csv_file:
    :param header:
    :param db_file:
    :param table_name:
    :return:
    """
    conn = sqlite3.connect(db_file)
    df = pd.read_csv(csv_file)
    if header:
        df.columns = header
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    log.info(f'File {csv_file} loaded into table {table_name}')


def create_db(db_name, output_folder):
    out_file = path.join(output_folder, db_name + '.db')
    sqlite3.connect(out_file)
    log.info(f'SQLite database {db_name}.db created in {output_folder}')
    return out_file
