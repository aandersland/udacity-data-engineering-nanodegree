# Overview

Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different JSON files and formats. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, we will need to do the following:
* Design a Star schema database that is optimized for song play analysis using Postgres. The Fact table will be called songplays along with our Dimension tables: users, songs, artists, and time.
* Build an ETL pipeline using Python to parse the data from the JSON files (Songs and Logs).
* Populate the database tables with the parsed JSON data.

## Setup environment
* CD to project directory
* Run **python3 environment_setup.py**

## Setup Database
* Run **sudo su - postgres**
* Run **createdb studentdb**
* Run **psql**
* Run **create role student with login password 'student';**
* Run **alter role student createrole createdb;**
* Run **alter role student createrole;**
* Run **alter database studentdb owner to student;**
* Run **\q**
* Run **exit**

## Create Tables and Populate
* Run **python3 database/create_tables.py** 
* Run **python3 etl.py**
* Run **python3 database/test_inserts.py**

## Resources
* https://linuxhint.com/postgresql_python/
* http://www.postgresqltutorial.com/postgresql-upsert/
* http://www.postgresqltutorial.com/postgresql-data-types/
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
* https://erikrood.com/Python_References/extract_month_year_pandas_final.html
* https://stackoverflow.com/questions/34883101/pandas-converting-row-with-unix-timestamp-in-milliseconds-to-datetime
* https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.tolist.html
* https://www.journaldev.com/16140/python-system-command-os-subprocess-call
* https://www.poftut.com/delete-and-remove-file-and-directory-with-python/
* https://stackoverflow.com/questions/6943208/activate-a-virtualenv-with-a-python-script
* http://docs.python-requests.org/en/master/user/quickstart/
