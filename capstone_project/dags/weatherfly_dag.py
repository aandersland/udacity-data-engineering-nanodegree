from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               CreateTablesInRedshiftOperator,
                               AirportNameTranslate)
from helpers import SqlQueries


default_args = {
    'owner': 'weatherfly',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('weatherfly_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2019, 9, 8),
          end_date=datetime(2019, 9, 10),
          schedule_interval=None,
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_stage_flight_details_table = CreateTablesInRedshiftOperator(
    task_id='create_stage_flight_details_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_staging_flights,
    drop_table=True,
    table='stage_flight_details',
    provide_context=True
)

create_stage_airports_table = CreateTablesInRedshiftOperator(
    task_id='create_stage_airports_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_staging_airports,
    drop_table=True,
    table='stage_airports',
    provide_context=True
)

create_stage_weather_table = CreateTablesInRedshiftOperator(
    task_id='create_stage_weather_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_staging_weather,
    drop_table=True,
    table='stage_weather',
    provide_context=True
)

create_f_flights_table = CreateTablesInRedshiftOperator(
    task_id='create_f_flights_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_f_flights,
    drop_table=True,
    table='f_flights',
    provide_context=True
)

create_d_flight_detail_table = CreateTablesInRedshiftOperator(
    task_id='create_d_flight_detail_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_d_flight_detail,
    drop_table=True,
    table='d_flight_detail',
    provide_context=True
)

create_d_time_table = CreateTablesInRedshiftOperator(
    task_id='create_d_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_d_time,
    drop_table=True,
    table='d_time',
    provide_context=True
)

create_d_weather_table = CreateTablesInRedshiftOperator(
    task_id='create_d_weather_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_d_weather,
    drop_table=True,
    table='d_weather',
    provide_context=True
)

create_d_airport_table = CreateTablesInRedshiftOperator(
    task_id='create_d_airport_table',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_d_airport,
    drop_table=True,
    table='d_airport',
    provide_context=True
)

create_weather_airport_name_translate = CreateTablesInRedshiftOperator(
    task_id='create_weather_airport_name_translate',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.create_weather_airport_name_translate,
    drop_table=True,
    table='weather_airport_name_translate',
    provide_context=True
)

table_created_operator = DummyOperator(task_id='Create_Table_execution',
                                       dag=dag)

stage_flight_details_to_redshift = StageToRedshiftOperator(
    task_id='stage_flight_details',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_flight_details',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='inbound/flight_data',
    aws_region='us-east-1',
    time_format='auto',
    comp_update='off',
    stat_update='off',
    format_one='delimiter',
    format_two=',',
    format_three=" ignoreheader as 1 acceptinvchars as ' ' bzip2;",
    provide_context=True
)

stage_weather_to_redshift = StageToRedshiftOperator(
    task_id='stage_weather',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_weather',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='inbound/weather_data',
    aws_region='us-east-1',
    time_format='auto',
    comp_update='off',
    stat_update='off',
    format_one='format as JSON ',
    format_two='auto',
    format_three='',
    provide_context=True
)

stage_airports_to_redshift = StageToRedshiftOperator(
    task_id='stage_airports',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_airports',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='inbound/airport_data',
    aws_region='us-east-1',
    time_format='auto',
    comp_update='off',
    stat_update='off',
    format_one='delimiter',
    format_two=',',
    format_three='removequotes escape ignoreheader as 1;',
    provide_context=True
)

stage_weather_airport_name_translate_to_redshift = StageToRedshiftOperator(
    task_id='stage_weather_airport_name_translate',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='weather_airport_name_translate',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='inbound/weather_airport_name_xref',
    aws_region='us-east-1',
    time_format='auto',
    comp_update='off',
    stat_update='off',
    format_one='delimiter',
    format_two=',',
    format_three='removequotes escape ignoreheader as 1;',
    provide_context=True
)

stage_data_operator = DummyOperator(task_id='Stage_Data_execution',
                                    dag=dag)

weather_airport_name_translate = AirportNameTranslate(
    task_id='weather_airport_name_translate',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    sql_statement=SqlQueries.weather_airport_name_translate,
    table='d_weather',
    provide_context=True
)

load_flights_table = LoadFactOperator(
    task_id='Load_flights_fact_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='f_flights',
    sql_statement=SqlQueries.f_flights,
    dag=dag,
    provide_context=True
)

load_flight_detail_dimension_table = LoadDimensionOperator(
    task_id='Load_flight_detail_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_flight_detail',
    sql_statement=SqlQueries.d_flight_detail,
    truncate_table=True,
    dag=dag,
    provide_context=True
)

load_weather_dimension_table = LoadDimensionOperator(
    task_id='Load_weather_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_weather',
    sql_statement=SqlQueries.d_weather,
    truncate_table=True,
    dag=dag,
    provide_context=True
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_airport',
    sql_statement=SqlQueries.d_airport,
    truncate_table=True,
    dag=dag,
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_time',
    sql_statement=SqlQueries.d_time,
    truncate_table=True,
    dag=dag,
    provide_context=True
)

data_loaded_operator = DummyOperator(task_id='Data_Loaded_execution',
                                     dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    tables=['f_flights', 'd_flight_detail',
            'd_time', 'd_weather', 'd_airport'],
    years=[1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998,
           1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008],
    dag=dag,
    provide_context=True
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

start_operator >> [create_stage_airports_table,
                   create_stage_flight_details_table,
                   create_stage_weather_table,
                   create_f_flights_table,
                   create_d_flight_detail_table,
                   create_d_airport_table,
                   create_d_weather_table,
                   create_d_time_table,
                   create_weather_airport_name_translate]

# staging tables
create_stage_airports_table >> table_created_operator

create_stage_flight_details_table >> table_created_operator

create_stage_weather_table >> table_created_operator

create_weather_airport_name_translate >> table_created_operator

# airport path
[create_stage_airports_table,
 create_d_airport_table] >> table_created_operator

table_created_operator >> stage_airports_to_redshift
stage_airports_to_redshift >> stage_data_operator

# weather path
[create_stage_weather_table,
 create_d_weather_table] >> table_created_operator

table_created_operator >> [stage_weather_to_redshift,
                           stage_weather_airport_name_translate_to_redshift]

[stage_weather_to_redshift,
 stage_weather_airport_name_translate_to_redshift] \
 >> weather_airport_name_translate
weather_airport_name_translate >> stage_data_operator

# flight detail path
[create_stage_flight_details_table,
 create_d_flight_detail_table] >> table_created_operator

table_created_operator >> stage_flight_details_to_redshift

stage_flight_details_to_redshift >> stage_data_operator

# time path
create_d_time_table >> table_created_operator

# load_flight_detail_dimension_table >> stage_data_operator

# flights path
create_f_flights_table >> table_created_operator

stage_data_operator >> [load_time_dimension_table,
                        load_airport_dimension_table,
                        load_flight_detail_dimension_table,
                        load_weather_dimension_table]

[load_time_dimension_table,
 load_airport_dimension_table,
 load_flight_detail_dimension_table,
 load_weather_dimension_table] >> data_loaded_operator

data_loaded_operator >> load_flights_table

# data quality checks path
load_flights_table >> run_quality_checks

run_quality_checks >> end_operator
