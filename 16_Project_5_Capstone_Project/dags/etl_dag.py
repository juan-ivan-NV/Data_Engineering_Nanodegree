from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

"""from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
"""

# SASValueToRedshiftOperator
from airflow.operators import (CopyToRedshiftOperator, DataQualityOperator)


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('etl_dag.py',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


""" ////////// 1 ► Operators and parameters to load data //////////"""
#     path = './sas_data'
sas_source_code_to_redshift = CopyToRedshiftOperator(
    redshift_conn_id = 'redshift',
    task_id='copy_immigrations_table',
    dag=dag,
    table = 'immigrations',
    s3_bucket = 'capstoneprojectde',
    s3_key = 'S3_data/immigrations_data.csv',
    aws_credentials_id = 'aws_credentials'
)

#     path = 'airport-codes_csv.csv'
airports_data_to_redshift = CopyToRedshiftOperator(
    task_id='copy_airports_table',
    dag=dag,
    table = 'airports',
    s3_bucket = 'capstoneprojectde',
    s3_key = 'S3_data/airports_data.csv',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials'
)

#    path = 'us-cities-demographics.csv'
demographics_data_to_redshift = CopyToRedshiftOperator(
    task_id='copy_demographics_table',
    dag=dag,
    table = 'demographics',
    s3_bucket = 'capstoneprojectde',
    s3_key = 'S3_data/demographics_data.csv',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials'
)

#    path = '../../data2/GlobalLandTemperaturesByCity.csv'
temperatures_data_to_redshift = CopyToRedshiftOperator(
    task_id='copy_temperatures_table',
    dag=dag,
    table = 'temperatures',
    s3_bucket = 'capstoneprojectde',
    s3_key = 'S3_data/Temperature_data.csv',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials'
)


""" ////////// 2 ► Operators and parameters to check data ////////// """

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    tables = [
        "immigrations",
        "airports",
        "demographics",
        "temperatures"
        ],
)


""" ////////// 3 ► Operator to finish the dag ////////// """

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


""" Starting and loading the tables from the local files """

start_operator >> sas_source_code_to_redshift
start_operator >> airports_data_to_redshift
start_operator >> demographics_data_to_redshift
start_operator >> temperatures_data_to_redshift


""" Data quality stage """

sas_source_code_to_redshift   >> run_quality_checks
airports_data_to_redshift     >> run_quality_checks
demographics_data_to_redshift >> run_quality_checks
temperatures_data_to_redshift >> run_quality_checks


""" End of the DAG execution """

run_quality_checks >> end_operator
