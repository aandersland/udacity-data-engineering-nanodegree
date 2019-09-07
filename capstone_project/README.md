
## Project Summary
Weatherfly is a new startup company in stealth mode that is looking to improve airline travel for businesses and families. Flight delays are known to many who travel across the United States and is an area with many promising opportunities. In order to prove out their business model they need to be able to reliably predict when delays are likely to occur. They have asked our company to setup a production anaytics environment for them to run their analysis and machine learning models.

## Project Scope

## Data Exploration
Flight Data -
* 1987 - http://stat-computing.org/dataexpo/2009/the-data.html
* 2003 - http://stat-computing.org/dataexpo/2009/the-data.html

Airport Data -
* http://stat-computing.org/dataexpo/2009/supplemental-data.html
* no https://openflights.org/data.html#airport
* no https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat

Weather Data -
* http://www.rcc-acis.org/docs_webservices.html
* http://jsfiddle.net/KEggleston/TrZY4/
* http://builder.rcc-acis.org/index2.html

## Data Model

## ETL

## How to Run
1. Setup an [AWS account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/) and [create a user](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started_create-admin-group.html) that has read access to S3 and admin rights on Redshift.
2. Start up a Redshift cluster and grant your user (awsuser) access.
![Redshift Cluster](AWS_Cluster.png)
3. In the Redshift Query Editor run each create statement in the create_tables.sql file.
![Redshift Query Editor](Create_Tables.png)
3. Run **python3 venv_setup.py**
4. Run **source venv/bin/activate**, you should see (venv) at the beginning of the line.
5. Run **pwd** and copy the path
6. Run **export AIRFLOW_HOME=(paste from step 5)/airflow**
7. Run **airflow initdb**
7. Open the ./airflow/airflow.cfg and set the following:
  * dags_folder = ./dags
  * plugins_folder = ./plugins
8. Create new terminal and run **source venv/bin/activate** then **airflow webserver -p 8080**
9. Create new terminal and run **source venv/bin/activate** then **airflow scheduler**
10. Navigate to http://localhost:8080/admin/variable/ and create a new one with the details:
  * Key = s3_bucket
  * Val = udacity-dend
11. Navigate to http://localhost:8080/admin/connection/ and create a new connection with the details:
  * Conn Id = aws_credentials
  * Conn Type = Amazon Web Services
  * Login = AWS Key from your IAM user credentials
  * Password = AWS Secret from your IAM user credentials
12. Navigate to http://localhost:8080/admin/connection/ and create a new connection with the details:
  * Conn Id = redshift
  * Conn Type = Postgres
  * Host = (enter your Redshift host path here without the port number)
  * Login = awsuser
  * Password = (awsuser password)
  * Port = 5439
13. Navigate to http://localhost:8080/admin/airflow/tree?dag_id=sparkify_dag and toggle the DAG to On in the upper left. Wait for the schedule to run or manually trigger.
![Airflow DAG](Airflow_DAG.png)


## Run
$ python -m venv projectname
$ source projectname/bin/activate
(venv) $ pip install ipykernel
(venv) $ ipython kernel install --user --name=projectname

## Resources
* https://stackoverflow.com/questions/18885175/read-a-zipped-file-as-a-pandas-dataframe
* https://anbasile.github.io/programming/2017/06/25/jupyter-venv/
* https://github.com/pandas-profiling/pandas-profiling
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.describe.html
* https://stackoverflow.com/questions/49505872/read-json-to-pandas-dataframe-valueerror-mixing-dicts-with-non-series-may-lea
* https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.json_normalize.html
* https://github.com/pandas-profiling/pandas-profiling
* https://www.50states.com/abbreviations.htm
* https://stackabuse.com/download-files-with-python/
* https://stackoverflow.com/questions/3964681/find-all-files-in-a-directory-with-extension-txt-in-python
* https://dbader.org/blog/python-parallel-computing-in-60-seconds
* https://stackoverflow.com/questions/5442910/python-multiprocessing-pool-map-for-multiple-arguments
* https://medium.com/datareply/airflow-lesser-known-tips-tricks-and-best-practises-cf4d4a90f8f
* https://stackoverflow.com/questions/27964134/change-value-in-ini-file-using-configparser-python
* https://www.tecmint.com/linux-curl-command-examples/
* https://docs.python.org/2/library/multiprocessing.html
* https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
* https://docs.aws.amazon.com/redshift/latest/dg/t_loading-gzip-compressed-data-files-from-S3.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_CAST_function.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_concat_op.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_DATEDIFF_function.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html
* https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html
* https://docs.aws.amazon.com/redshift/latest/dg/r_INITCAP.html


## Data Model

### f_flights
* flights_id (integer)
* flight_detail_id (integer)
* airport_depart_id (integer)
* airport_depart_weather_id (integer)
* depart_time_id (integer)
* airport_arrival_id (integer)
* airport_arrival_weather_id (integer)
* arrival_time_id (integer)
* schdld_flight_time_sec (integer)
* schdld_flight_time_min (integer)
* flight_time_sec (integer)
* flight_time_min (integer)
* depart_delay_sec (integer)
* depart_delay_min (integer)
* arrival_delay_sec (integer)
* arrival_delay_min (integer)

### d_flight_detail
* flight_detail_id (integer)
* carrier (string)
* origin (string)
* dest (string)
* distance (integer)
* schdld_depart_time (datetime)
* schdld_arrival_time (datetime)
* cancelled (integer)
* diverted (integer)

### d_time
* time_id (integer)
* date (date)
* datetime (datetime)
* timezone (string)
* year (integer)
* quarter (integer)
* month (integer)
* day (integer)
* day_of_week (integer)
* hour (integer)
* minute (integer)
* second (integer)

### d_weather
* weather_id (integer)
* date (date)
* max_temp (integer)
* min_temp (integer)
* avg_temp (integer)
* precipitation_in (integer)
* snow_fall_in (integer)
* snow_depth_in (integer)

### d_airport
* name (string)
* airport_code (string)
* city (string)
* state (string)
* country (string)
* latitude (float)
* longitude (float)