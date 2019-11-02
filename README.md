# Overview

This repo contains the projects for the Data Engineering Nanodegree program from [Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Project 1 - [Data Modeling with PostgreSql](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_modeling_postgresql) - Complete

Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different JSON files and formats. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, I did the following:
* Design a Star schema database that is optimized for song play analysis using Postgres. The Fact table will be called songplays along with our Dimension tables: users, songs, artists, and time. This will enable the analytics team to quickly query the data for their analysis.
* Build an ETL pipeline to parse the data from the JSON files (Songs and Logs). Here we will use Python and Pandas to parse and cleanup the data.
* Finally, we will populate the database tables using Python and prepared statements.

Technologies used: Python, Postgres, Pandas, JSON

## Project 2 - [Data Modeling with Apache Cassandra](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_modeling_apache_cassandra) - Complete
Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different CSV files. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, I did the following:
* Design a database model in Cassandra that is optimized for specific analytics queries and has partition keys and clustering columns. 
* Build an ETL pipeline using Python to parse the data from the CSV files.
* Populate the database tables with the parsed CSV data.

Technologies used: Python, Pandas, Apache Cassandra

## Project 3 - [Cloud Data Warehouses](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/cloud_data_warehouses) - Complete
Sparkify is a new startup company that released a new music streaming app. Their user base has increased and have outgrown their current architecture. They would like to move their operations to the cloud to take full advantage of their dataset and grow the company further.

In order to meet the companies needs, I did the following:
* Design a data warehouse in Amazon Redshift.
* Create an Amazon cluster through Python (Infrastructure as Code).
* Build a Python ETL pipeline to stage the data from S3 into Redshift.
* Perform the any necessary transformations and load the data into a set of dimensional data warehouse tables.
* Run the necessary queries against the data warehouse data.

Technologies used: Python, S3, IAM, VPC, EC2, RDS, PostgreSql


## Project 4 - [Data Lake with Spark](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_lake_spark) - Complete
Sparkify is a new startup company that released a new music streaming app. Their user base has increased and have outgrown their current architecture. They would like to move their data warehouse into a data lake in the cloud to take full advantage of their dataset and grow the company further.
In order to meet the companies needs, I did the following:
* Extract JSON logs on user activity from S3
* Extract JSON metadata on songs from S3 
* Build a Spark process to load data into analytic tables
* Upload the results back into S3
* Execute the Spark process on an AWS cluster

Technologies used: Python, SparkSQL, Spark Dataframes, Spark WebUI, S3, EMR, Athena, Amazon Glue


## Project 5 - [Data Pipelines with Apache Airflow](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_pipelines_apache_airflow) - Complete
Sparkify is a new startup company that released a new music streaming app. Their user base has increased and their architecture footprint is also increasing. To manage this infrastructure they are looking to increase the automation and monitoring of their data pipelines. They have chosen to use Apache Airflow as their tool of choice.

In order to meet the companies needs, I did the following:
* Design a data warehouse in Amazon Redshift
* Build high grade data pipelines that are: dynamic, reusable, and include monitoring
* Automate those pipelines by configuring and scheduling them in Airflow
* Monitor and debug the Airflow pipelines
* Include data quality checks on all data pipelines
* Stage data from S3 into Redshift staging tables
* Load data from Redshift staging tables into final fact/dimension tables

Technologies used: Python, Apache Airflow, Redshift, S3


## [Capstone Project](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/capstone_project) - Complete
Weatherfly is a new startup company in stealth mode that is looking to improve airline travel for businesses and families. Flight delays are known to many who travel across the United States and is an area with many promising opportunities. In order to prove out their business model they need to be able to reliably predict when delays are likely to occur. They have asked our company to setup a production analytics environment for them to run their analysis and machine learning models.

The scope is to build an analytics platform on AWS Redshift for the Weatherfly team to run queries against. The goal is to have 20 years worth of flight and weather data to support their analysis. In order to meet these goals, we need to do the following:
* Find a data source that has 20 years worth of US flight data and corresponding airport data. To determine how weather affects travel we will also need to get the associated weather data for these airports over this same time period.
* The data will need to be cleaned up and prepared for the data model.
* In order to ensure we have fast queries, we will use a Fact and Dimmension model.
* The data will be uploaded to S3 and loaded into stage tables in Redshift. From there we will load into the Fact / Dimmension tables.
* The ETL pipeline will be built using Airflow to schedule and coordinate the activities.

Technologies used: Python, Apache Airflow, S3, Redshift

## References:
Project definitions - https://www.udacity.com/course/data-engineer-nanodegree--nd027
