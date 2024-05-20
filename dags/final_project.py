from datetime import datetime, timedelta
import pendulum
import os
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'gracomot',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_tables = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql="create_tables.sql",
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_events",
        s3_bucket = "gracomot-airflow",
        s3_key = "log-data",
        json_format_file = "log_json_path.json"
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table = "staging_songs",
        s3_bucket = "gracomot-airflow",
        s3_key = "song-data",
    )

  

    load_songplay_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = "redshift",
        table="songplays",
        load_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_conn_id = "redshift",
        truncate_records = True,
        load_query=SqlQueries.user_table_insert
    )
  
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = "redshift",
        table='songs',
        truncate_records = True,
        load_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = "redshift",
        table='artists',
        truncate_records = True,
        load_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = "redshift",
        table='time',
        truncate_records = True,
        load_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        tables = ["songs", "users", "artists", "time"]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >>  create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplay_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator

final_project_dag = final_project()