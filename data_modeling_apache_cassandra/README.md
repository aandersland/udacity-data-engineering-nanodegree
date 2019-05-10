# Overview

Sparkify is a new startup company that released a new music streaming app. Their analytics team is interested in understanding what songs users are listening too. Unfortunately this data is scattered across different CSV files. In order for the Sparkify team to understand their users better, this data will need to be placed into a database that allows easier access to the data and is optimized for analytic queries.

In order to meet the analytics teams needs, we will need to do the following:
* Design a database model in Cassandra that is optimized for specific analytics queries and has partition keys and clustering columns. 
* Build an ETL pipeline using Python to parse the data from the CSV files.
* Populate the database tables with the parsed CSV data.

## Setup environment
* CD to project directory
* Run **python3 environment_setup.py**
* Run **service cassandra start**
* Run **service --status-all | grep cassandra**
* Verify the cassandra is running (+ means the service is running and - means it is stopped)
* Additional details can be found here if it is not running: http://cassandra.apache.org/download/

## Setup Database
Follow these instructions to setup a Cassandra Docker instance: https://hub.docker.com/_/cassandra.
Once installed run **sudo docker run --name cass-sparkify --network host -d cassandra:latest**

## Create Tables and Populate data
* Run **python3 etl.py**

## Resources
* https://docs.python.org/3/library/subprocess.html#subprocess.run
* http://cassandra.apache.org/download/
* https://www.simplilearn.com/cassandra-installation-and-configuration-tutorial-video
* https://docs.docker.com/install/linux/docker-ce/ubuntu/
* https://datastax.github.io/python-driver/installation.html
* http://datastax.github.io/python-driver/getting_started.html
* https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateTable.html
