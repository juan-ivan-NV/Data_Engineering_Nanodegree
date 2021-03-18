# Data Modeling with Postgres

## Who Will Benefit From This Project?

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

## What Are Your Goals and Objectives?

1.- To create a Postgres DB schema and an ETL pipeline to obtain tables designed for optimizing queries on song play analysis, so we will focus on developing and complete different scripts described below. 

## What is the purpuse of each script?

* test.ipynb ► Displays the first few rows of each table to let you check your DB.
* create_tables.py ► To delete, create or reset your tables.
* etl.ipynb ► To read and process a single file from song_data and log_data and loads the data into DB tables.
* etl.py ► To read and process files from song_data and log_data and loads them into your tables.
* sql_queries.py ► Contains all your sql queries to create tables and insert data.
* README.md ► General description of this project.

## How to run the Python scripts?

In release order

1.-> In the terminal type "python create_tables.py" to create the tables in the DB.

2.-> In a terminal type "python etl.py" ETL processes for each table, it also shows the tables and some info.

3.-> To display all tables open test.ipynb and run all cells.

## Tables:

Fact table:

* Songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

Dimension Tables:

* users: user_id, first_name, last_name, gender, level.
* songs: song_id, title, artist_id, year, duration.
* artists: artist_id, name, location, latitude, longitude.
* time: start_time, hour, day, week, month, year, weekday.





