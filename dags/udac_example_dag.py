import os
from datetime import datetime, timedelta
from helpers import SqlQueries
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)



default_args = {
   'owner': 'peter',
   'start_date': datetime(2019, 1, 12),
   'depends_on_past': False,
   'retries': 3,
   'retry_delay': timedelta(seconds=300),
   'catchup': False,
   'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A',
    copy_options="JSON 'auto'"
)    

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.songplay_table_insert,
    table='songplays',
    append_data=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.user_table_insert,
    table='users',
    append_data=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.song_table_insert,
    table='songs',
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.artist_table_insert,
    table='artists',
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.time_table_insert,
    table='time',
    append_data=False
    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_stmt='SELECT COUNT(*) FROM songs WHERE songid IS NULL',
    expected_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



#AIRFLOW PIPELINES
start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift 

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table

load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

