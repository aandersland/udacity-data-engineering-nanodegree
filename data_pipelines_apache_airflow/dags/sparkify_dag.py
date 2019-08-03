from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2019, 8, 2),
          end_date=datetime(2019, 8, 4),
          schedule_interval='@hourly',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='log_data',
    aws_region='us-west-2',
    time_format='epochmillisecs',
    comp_update='off',
    stat_update='off',
    format_json='s3://udacity-dend/log_json_path.json',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='song_data',
    aws_region='us-west-2',
    time_format='epochmillisecs',
    comp_update='off',
    stat_update='off',
    format_json='auto',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songplays',
    sql_statement=SqlQueries.songplay_table_insert,
    dag=dag,
    provide_context=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='users',
    sql_statement=SqlQueries.user_table_insert,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='songs',
    sql_statement=SqlQueries.song_table_insert,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='artists',
    sql_statement=SqlQueries.artist_table_insert,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='time',
    sql_statement=SqlQueries.time_table_insert,
    truncate_table=False,
    dag=dag,
    provide_context=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    dag=dag,
    provide_context=True
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_time_dimension_table,
                         load_artist_dimension_table,
                         load_user_dimension_table,
                         load_song_dimension_table]

[load_time_dimension_table,
 load_artist_dimension_table,
 load_user_dimension_table,
 load_song_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
