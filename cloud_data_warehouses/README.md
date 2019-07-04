CHECK TO MAKE SURE KEYS ARE NOT INCLUDED

# Table Creation
CRITERIA
MEETS SPECIFICATIONS
Table creation script runs without errors.

The script, create_tables.py, runs in the terminal without errors. The script successfully connects to the Sparkify database, drops any tables if they exist, and creates the tables.

Staging tables are properly defined.

CREATE statements in sql_queries.py specify all columns for both the songs and logs staging tables with the right data types and conditions.

Fact and dimensional tables for a star schema are properly defined.

CREATE statements in sql_queries.py specify all columns for each of the five tables with the right data types and conditions.

# ETL
ETL script runs without errors.

The script, etl.py, runs in the terminal without errors. The script connects to the Sparkify redshift database, loads log_data and song_data into staging tables, and transforms them into the five tables.

ETL script properly processes transformations in Python.

INSERT statements are correctly written for each table and handles duplicate records where appropriate. Both staging tables are used to insert data into the songplays table.

# Code Quality

The project shows proper use of documentation.

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

The project code is clean and modular.

Scripts have an intuitive, easy-to-follow structure with code separated into logical functions. Naming for variables and functions follows the PEP8 style guidelines.


--------------------------------------------------

# Overview

Sparkify is a new startup company that released a new music streaming app. Their user base has increased and have outgrown their current architecture. They would like to move their operations to the cloud to take full advantage of their dataset and grow the company further.

In order to meet the companies needs, we will need to do the following:
* Design a data warehouse in the Amazon cloud. 
* Build an ETL pipeline to stage the data from S3 into Redshift, perform the necessary transformations, and load the data into a data warehouse.

## Setup environment


## Setup Database


## Create Tables and Populate data


## Resources
* https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html
* https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.describe_clusters
* https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

