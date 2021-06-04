# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

For this project is expected to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Also as data quality should be added for analyses and tests against the datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project overview

* To create custom operators to perform tasks such as staging the data.
* To fill the data warehouse.
* To run checks on the data.

All SQL transformations are provided but should be 