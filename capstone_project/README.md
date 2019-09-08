
## Project Summary
Weatherfly is a new startup company in stealth mode that is looking to improve airline travel for businesses and families. Flight delays are known to many who travel across the United States and is an area with many promising opportunities. In order to prove out their business model they need to be able to reliably predict when delays are likely to occur. They have asked our company to setup a production analytics environment for them to run their analysis and machine learning models.

## Scope and Goals
The scope is to build an analytics platform on AWS Redshift for the Weatherfly team to run queries against. The goal is to have 20 years worth of flight and weather data to support their analysis. In order to meet these goals, we need to do the following:
1. Find a data source that has 20 years worth of US flight data and corresponding airport data. To determine how weather affects travel we will also need to get the associated weather data for these airports over this same time period.
2. The data will need to be cleaned up and prepared for the data model.
3. In order to ensure we have fast queries, we will use a Fact and Dimmension model.
4. The data will be uploaded to S3 and loaded into stage tables in Redshift. From there we will load into the Fact / Dimmension tables.
5. The ETL pipeline will be built using Airflow to schedule and coordinate the activities.

## Gather Data
After doing some extensive online searches for the data, I arrived at the following sources of data to meet our goals. 

**Flights -** The site stat-computing.org had a great [flight data set](http://stat-computing.org/dataexpo/2009/the-data.html) for the years of 1987 to 2008. These files were bz2 compressed csv files containing US flight data for each year.

**Airports -** Originally, I used the supplemental [airport data](http://stat-computing.org/dataexpo/2009/supplemental-data.html) provided along with the flight data, however I moved away from that later in the project. This is the [airport data set](https://openflights.org/data.html#airport), I ended up using as it had more information and better names from openflights.org. 

**Weather -**
The [Applied Climate Information System](http://www.rcc-acis.org/docs_webservices.html) site had a great API to pull detailed weather data. It had detailed documentation and great set of examples to work off of. 

**Download Data -** All three of these datasets were pulled down locally for analysis using the gather_datasets.py script. This allowed us to use a Jupyter Notebook to explore the data sets and identify some of the data issues we would need to fix. The script can be configured to use sequential or parallel processing to download the data depending on your system (see the configs.cfg for more details).

## Data Exploration
In our exploration of the data we noted several areas that needed to be cleaned up before we could process the data into our model. See the [Jupyter Notebook]() or the [SQL statements]() for more details.

**Key Data Decisions -**
 * Only use US International airports and corresponding flight/weather data - This decision was made due to the fact that we did not have adequate flight/weather data for the smaller airports across the US.
 * 

**Flight Data -**
 * Include only US international airport flights
 * Convert date and time fields to a single datetime field - special handling was needed due to invalid values (i.e. 95 minutes or the time 2525)
 * Text in numerical columns - Replace with null
 
**Airport Data -**
 * Text with whitespaces - Trim fields on upload
 * Filter out non-USA flight data
 * Rename columns to be more meaning full
 * Null fields - removed from final data set
 * Include only US international airports - 

**Weather Data -**
 * Text in numerical columns - Replaced with 0's
 Include only weather data for US international airports
  
## Data Model
* Use a fact/dimmension model for speed of analytic queries
* pk, distkey, sortkey
* data dictionary
* Identify table definitions, joins, primary keys, sort keys, distribution keys
* 
(capstone_data_model.png)

## ETL
* pipeline pic
* Detail each step
* dq checks (create check for all years, integrity constraints, stage/table counts)
* queries to run

Project Write Up
Detail why each tech was chosen
Data updates how often and why

final dataset results

Future state scenarios
If the data was increased by 100x:
If the pipelines were run on a daily basis by 7am:
If the database needed to be accessed by 100+ people:





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
* https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-acceptinvchars
*
