# quote-events-etl
ETL to extract and enrich quote events


## Overview

This repository contains the code to run the ETL to extract and load quote events and Companies House data extract 
used to enrich quotes data. The solution can be run using different 
configurations injected through a config YAML file.

SQLite database can be browsed using [SQLite Browser](https://sqlitebrowser.org/dl/) or other database client.


![diagram](img/diagram.jpg)

## Requirements

Code is developed in Python 3.7. Before running:

1- Set up and activate a Python 3.7 virtual environment
```
python3 -m venv /path/to/new/virtual/environment
source /path/to/new/virtual/environment/bin/activate
```

2 - Install required packages from the repository root
``
pip install -r requirements.txt
``

3 - Clone repository

4 - Edit the ``config.yaml`` file replacing the passwords


## Run the tests
From the root of the repository run ``pytest``


## Run the ETL
From the root of the repository run ``python3 quotes_etl.py config.yaml``
