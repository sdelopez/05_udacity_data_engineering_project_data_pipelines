from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# get S3 bucket path  from Airflow variables
# S3 bucket path can be set/modified in Airflow variables

s3_bucket = Variable.get('s3_bucket')
s3_prefix_log = Variable.get('s3_prefix_log')
s3_prefix_song = Variable.get('s3_prefix_song')

# define defautl argument for DAG
default_args = {
    'owner': 'sdelopez',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
}


dag = DAG('02_sparkify_etl_dag',
          default_args=default_args,
          description='Extract from S3, Transform and Load data in Redshift with Airflow pipeline',
          schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Define task to Extract songs and events data to staging table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket=s3_bucket,
    s3_prefix=s3_prefix_log,
    table='staging_events',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket=s3_bucket,
    s3_prefix=s3_prefix_song,
    table='staging_songs',
    copy_options="FORMAT AS JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.songplays_table_insert,
    append_data = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    select_sql=SqlQueries.users_table_insert,
    append_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    select_sql=SqlQueries.songs_table_insert,
    append_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    select_sql=SqlQueries.artists_table_insert,
    append_data = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    select_sql=SqlQueries.time_table_insert,
    append_data = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
                {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
                { 'check_sql': "SELECT COUNT(*) FROM artists WHERE name IS NULL", 'expected_result': 0 },
                { 'check_sql': "SELECT COUNT(*) FROM time WHERE weekday IS NULL", 'expected_result': 0 },
                { 'check_sql': "SELECT COUNT(*) FROM songplays WHERE songid IS NULL", 'expected_result': 0 }
                ],
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define tasks DAG for Sparkify ETL process


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator