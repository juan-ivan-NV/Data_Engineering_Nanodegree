# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Description 

You will need to perform an ETL process to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Datasets

* Song data: s3://udacity-dend/song_data

Files that contain information like:
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

* Log data: s3://udacity-dend/log_data
![Image](../Images/log-data.png)

## Templates

* etl.py ► reads data from S3, processes that data using Spark, and writes them back to S3
* dl.cfg ► contains your AWS credentials
* README.md ► provides discussion on your process and decisions
 
