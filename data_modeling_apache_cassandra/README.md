query , pk, composite key for uniqueness
when determining the partition key, determine an even spread
clustering columns determine sort order asc
PK = ((year), artist, album)

You can use as many clustering columns. You cannot use the clustering columns out of order in the SELECT statement. You may choose to omit using a clustering column in your SELECT statement. That's OK. Just remember to use them in order when you are using the SELECT statement.

WHERE clause
Data Modeling in Apache Cassandra is query focused, and that focus needs to be on the WHERE clause
Failure to include a WHERE clause will result in an error

you cannot where a column not in the pk, without specifiying all the pks first. then it makes more sense to select column where all pk's

# Overview

Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different CSV files. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, we will need to do the following:
* Design a database model in Cassandra that is optimized for specific analytics queries. 
* Build an ETL pipeline using Python to parse the data from the CSV files.
* Populate the database tables with the parsed CSV data.

## Setup environment
* CD to project directory
* Run **python3 environment_setup.py**

## Setup Database


## Create Tables and Populate
* Run **python3 database/create_tables.py** 
* Run **python3 etl.py**
* Run **python3 database/test_inserts.py**

## Resources
http://cassandra.apache.org/download/
https://datastax.github.io/python-driver/installation.html
http://datastax.github.io/python-driver/getting_started.html
https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateTable.html
