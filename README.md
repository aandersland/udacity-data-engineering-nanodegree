# Overview

This repo contains the projects for the Data Engineering Nanodegree program from [Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Project 1 - [Data Modeling with PostgreSql](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_modeling_postgresql) - Complete

Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different JSON files and formats. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, we will need to do the following:
* Design a Star schema database that is optimized for song play analysis using Postgres. The Fact table will be called songplays along with our Dimension tables: users, songs, artists, and time. This will enable the analytics team to quickly query the data for their analysis.
* Build an ETL pipeline to parse the data from the JSON files (Songs and Logs). Here we will use Python and Pandas to parse and cleanup the data.
* Finally, we will populate the database tables using Python and prepared statements.

Technologies used: Python, Postgres, Pandas, JSON

## Project 2 - [Data Modeling with Apache Cassandra](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_modeling_apache_cassandra) - In-progress
Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different CSV files. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, we will need to do the following:
* Design a database model in Cassandra that is optimized for specific analytics queries and has partition keys and clustering columns. 
* Build an ETL pipeline using Python to parse the data from the CSV files.
* Populate the database tables with the parsed CSV data.

Technologies used: Python, Pandas, Apache Cassandra

## Project 3 - [Cloud Data Warehouses](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/cloud_data_warehouses) - Not started
In this project I built an ETL pipeline that enables the analytics team at Sparkify to gain insights into song usage by their customers. This was accomplished by the following:
* Setup AWS infrastructure using IaC
* Extract data from S3
* Stage data in Redshift
* Transform data into dimensional tables for analytics

Technologies used: Python, S3, IAM, VPC, EC2, RDS, PostgreSql


## Project 4 - [Data Lake with Spark](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_lake_spark) - Not started
In this project I built an ETL pipeline to create a data lake for the Sparkify team. This was accomplished by the following:
* Extract JSON logs on user activity from S3
* Extract JSON metadata on songs from S3 
* Build a Spark process to load data into analytic tables
* Upload the results back into S3
* Execute the Spark process on an AWS cluster

Technologies used: Python, SparkSQL, Spark Dataframes, Spark WebUI, S3, EMR, Athena, Amazon Glue


## Project 5 - [Data Pipelines with Apache Airflow](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/data_pipelines_apache_airflow) - Not started
In this project I further enhanced the data pipelines for the Sparkify team. This was accomplished by the following:
* Create a new set of data pipelines
* Automate those pipelines by configuring and scheduling them in Airflow
* Monitor and debug the Airflow pipelines

Technologies used: Python, Apache Aiflow


## [Capstone Project](https://github.com/aandersland/udacity-data-engineering-nanodegree/tree/master/capstone_project) - Not started
Combine what you've learned throughout the program to build your own data engineering portfolio project.

Technologies used: Python

## References:
Project definitions - https://www.udacity.com/course/data-engineer-nanodegree--nd027
