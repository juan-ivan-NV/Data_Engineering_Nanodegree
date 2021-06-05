# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

For this project is expected to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Also as data quality should be added for analyses and tests against the datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project overview

* To create custom operators to perform tasks such as staging the data.
* To fill the data warehouse.
* To run checks on the data.

All SQL transformations are provided but should be executed with custom operators.

![Image](../Images/example-dag.png)

## Airflow enviroment

This project will be executer in the Udacity enviroment for Airflow

Following connections shoul be created:

* connection: aws_credentials ► Iam user credentials.
* connection: redshift ► Data from the cluster.

To turn on the Airflow enviroment <code>/opt/airflow/start.sh</code>

## Datasets

* Log data:<code>s3://udacity-dend/log_data</code>
* Log data:<code>s3://udacity-dend/song_data</code>

# Challenges

### Configuring the DAG

In the DAG, add <code>default parameters</code> according to these guidelines

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

## Building the operators

### Stage operator

It should be able to load any JSON formatted files from S3 to AWS Redshift.
The operator cretes and runs a SQL COPY statement based on the parameters provided.

The parameters should be used to distinguish between JSON file.

### Fact and Dimension Operators

Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on wich to run the query against.

### Data Quality Operator

This operator us implemented finally and is used to run checks on the data itself.
The main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests.

## Directory overview

dags ► dir
    * full_dag.py
plugins ► dir
    * helpers ► dir
        * __init__.py
        * sql_queries.py
    * operator ► dir
        * __init__.py
        * data_quality.py
        * load_dimension.py
        * load_fact.py
        * stage_redshift.py
    * __init__.py
create_tables.sql 
README.md
