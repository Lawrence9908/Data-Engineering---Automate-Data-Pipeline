import os
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

# Define constants
s3_bucket = "uc-de-airflow-aws"
events_s3_key = "log-data"
songs_s3_key = "song-data/A/A/"
log_json_file = 'log_json_path.json'

# Default arguments for the DAG
default_args = {
    'owner': 'Lawrence Mugwena',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    end_date=datetime(2024, 11, 30),
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    # Task to start the DAG execution
    start_operator = DummyOperator(task_id='Begin_execution')

    # Task to create Redshift tables
    create_redshift_tables = PostgresOperator(
        task_id='Create_tables',
        postgres_conn_id="redshift",
        sql='common/create_tables.sql'
    )

    # Staging events and songs to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key,
        log_json_file=log_json_file
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key
    )

    # Loading fact and dimension tables
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql_query=SqlQueries.time_table_insert
    )

    # Data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"]
    )

    # Task to end the DAG execution
    end_operator = DummyOperator(task_id='End_execution')

    # Define task dependencies
    start_operator >> create_redshift_tables >> [
        stage_events_to_redshift,
        stage_songs_to_redshift
    ] >> load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator

# Instantiate the DAG
final_project_dag = final_project()
