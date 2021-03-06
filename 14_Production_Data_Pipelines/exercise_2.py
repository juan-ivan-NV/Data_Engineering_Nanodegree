#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple PythonOperators
#3 - Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


#
# DONE: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#


def log_oldest():
    """To log the oldest from the table when this function is called"""
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")
        
        
dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)


def log_youngest():
    """To log the youngest from the table when this function is called"""
    redshift_hook = PostgresHook("redshift")

    # Find all trips where the rider was under 18
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

               
"""//////////Create and log oldest///////////////"""
create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)


log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)

"""//////////Create and log youngest///////////////"""
create_youngest_task = PostgresOperator(
    task_id = 'younger_riders_table',
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2002
        );
        COMMIT;
    """,
    postgres_conn_id = "redshift")


log_youngest_task = PythonOperator(
    task_id="log_youngest",
    dag=dag,
    python_callable=log_youngest
)


"""Find out how often each bike is ridden"""
lifetime_rides_task = PostgresOperator(
    task_id = "lifetime_rides",
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid)
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """,
    postgres_conn_id = "redshift")

"""Count the number of stations by city"""
city_station_count_task = PostgresOperator(
    task_id = "city_station_count",
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS (
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id = "redshift")


create_oldest_task >> log_oldest_task 
create_youngest_task >> log_youngest_task