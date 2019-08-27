
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
*

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