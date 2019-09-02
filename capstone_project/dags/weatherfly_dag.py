from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'weatherfly',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('weatherfly_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2019, 8, 2),
          end_date=datetime(2019, 8, 4),
          # schedule_interval='@hourly',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_flight_details_to_redshift = StageToRedshiftOperator(
    task_id='stage_flight_details',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_flight_details',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='flight_data',
    aws_region='us-west-2',
    time_format='epochmillisecs',
    comp_update='off',
    stat_update='off',
    format_json='s3://udacity-dend/log_json_path.json',
    provide_context=True
)

stage_weather_to_redshift = StageToRedshiftOperator(
    task_id='stage_weather',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_weather',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='weather_data',
    aws_region='us-west-2',
    time_format='epochmillisecs',
    comp_update='off',
    stat_update='off',
    format_json='auto',
    provide_context=True
)

stage_airports_to_redshift = StageToRedshiftOperator(
    task_id='stage_airports',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_airports',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='airport_data',
    aws_region='us-west-2',
    time_format='epochmillisecs',
    comp_update='off',
    stat_update='off',
    format_json='auto',
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
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_weather_dimension_table = LoadDimensionOperator(
    task_id='Load_weather_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_weather',
    sql_statement=SqlQueries.d_weather,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_airport',
    sql_statement=SqlQueries.d_airport,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='d_time',
    sql_statement=SqlQueries.d_time,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    tables=['f_flights', 'd_flight_detail', 'd_time', 'd_weather', 'd_airport'],
    dag=dag,
    provide_context=True
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

start_operator >> [stage_airports_to_redshift, stage_flight_details_to_redshift,
                   stage_weather_to_redshift]

stage_airports_to_redshift >> load_airport_dimension_table
stage_flight_details_to_redshift >> [load_flight_detail_dimension_table,
                                     load_time_dimension_table]
stage_weather_to_redshift >> load_weather_dimension_table

[load_time_dimension_table, load_airport_dimension_table,
 load_flight_detail_dimension_table,
 load_weather_dimension_table] >> load_flights_table

load_flights_table >> run_quality_checks

run_quality_checks >> end_operator
