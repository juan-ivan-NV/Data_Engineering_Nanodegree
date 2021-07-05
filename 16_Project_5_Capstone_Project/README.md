# Data engineering capstone project

## Data sources.

* <a href= "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/">US-cities-demographics.csv</a> / Demograpjic data for cities

<pre>Columns ► city | state | media_age | male_population | female_population | total_population | num_veterans | foreign_born | average_household_size | state_code | race | count</pre>

* <a href= "https://www.trade.gov/national-travel-and-tourism-office">immigration_data_sample.csv</a> / l94 imigration data

<pre>Columns ► cicid | year | month | cit | res | iata | arrdate | mode | addr | depdate | bir | visa | coun | dtadfil | visapost | occup | entdepa | entdepd | entdepu | matflag | biryear | dtaddto | gender | insnum | airline | admnum | fltno | visatype</pre>

* <a href= "https://datahub.io/core/airport-codes#data">airport-codes_csv.csv</a> / information related to airports

<pre>Columns ► ident | type | name | elevation_ft | continent | iso_country | iso_region | municipality | gps_code | iata_code | local_code | coordinates</pre>

* <a href= "https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data">GlobalLandTemperaturesByCity.csv</a> / Kaggle source (This study combines 1.6 billion temperature reports from 16 pre-existing archives)

<pre>Columns ► dt | AverageTemperature | AverageTemperatureUncertainity | City | Country | Latitude | Longitude</pre>

## Files overview

* capstone_project_notebook.ipynb ► For data cleaning and exploration.

* sql_queries.py ► Queries to CREATE the tables and COPY queries.

* create_tables.py ► Functions to execute the CREATE tables queries

* etl.py ► Functions to populate the tables from S3 to Redshift

* dwh.cfg ► It contains the credentials needed

* README.md ► Readme for general description.

* airport-codes_csv.csv ► Dataset airports

* immigration_data_sample.csv ► Dataset immigrations

* us-cities-demographics.csv ► Dataset demographics

* l94_SAS_Labels_Descriptions.SAS ► Labels from l94


## Data exploration.

This stage is to work with PySpark and look for the data frame information, such as nulls, dimentions, and types of data.

After taking a look at each dataframe it comes the desition to only keep usefull columns.

## Data selection and cleaning.

For the 4 files, only the columns containing data that I considered useless were removed as well as the columns with a lot of nulls or missing values, the whole process is on the <pre>capstone_project_notebook.ipynb</pre> using PySpark to manipulate dataframes, then the data were saved as CSV files and uploaded to and AWS S3 bucket.

![Image_1](Images/s3.PNG)

## Database creation to redshift.

After the DB creation with the command <pre>python create_tables.py</pre>

![Image_2](Images/create_tables.PNG)

The DB is populated with the files from S3 (using the COPY command) / 
<pre>python etl.py</pre>

![Image_3](Images/copy_csv.png)

## Query in the Redshift DB.

![Image_4](Images/q_airp.PNG)

![Image_5](Images/t_airp.PNG)

### Schema.

An overview of the whole process.

![Image_6](Images/over.PNG)

## Issues solved

Due to: 

* Some data types missmatch between the CSV files and the DB.

* A difference in the number of columns in the tables and CSV files.

It was needed to use the query "stl_load_errors errors" on the Redshift query interface to take a look at the errors.

![Image_7](Images/error.PNG)

## Complete Project Write Up

* What's the goal?

Integrate the knowledge gained trough the lessons to create this project.

* What queries will you want to run? / Why did you choose the model you chose?

For the DB we could get different info type about US immigrations / airports / demographics / temperatures and that info could be joined by location (state, country ...) except for the airports data wich coordinates should be transformed into state or city before performing joins by place, however that step takes too much time so I've skipped it.

* How would Spark or Airflow be incorporated?

For this project spark is used to work with dataframes because is faster than pandas.

Working with airflow the DAG would be something like this...

![Image_9](Images/copy_csv.png)

* Clearly state the rationale for the choice of tools and technologies for the project.

Python ► With this programming language it comes the utilization of different libraries to manipulate databases, files and data (configparser, psycopg2, os, pandas...)

PySpark ► PySpark is an interface for Apache Spark in Python. It not only allows you to write Spark applications using Python APIs, but also provides the PySpark shell for interactively analyzing your data in a distributed environment.

AWS_S3 ► is storage for the Internet. It is designed to make web-scale computing easier.

AWS_Redshift ► is a fully managed, petabyte-scale data warehouse service in the cloud. You can start with just a few hundred gigabytes of data and scale to a petabyte or more. This enables you to use your data to acquire new insights for your business and customers.

* Propose how often the data should be updated and why.

Depending on the ammount of data that is recorded in one day or the data latency requiered by the company, but for this type of data the records could be updated daily or weekly in order to keep the last updates in the ammount of immigranst or demographics data.

* Post your write-up and final data model in a GitHub repo.

<a href= "https://github.com/juan-ivan-NV/Data_Engineering_Nanodegree/tree/main/16_Project_5_Capstone_Project">Github project</a> 

* Include a description of how you would approach the problem differently under the following scenarios:

    * If the data was increased by 100x.
    
    For that reason the data is stored in Redshift warehouse, so the data can keep wrowing and also the cluster capacity.
    
    Another option could be Cassandra to write online transactions as it comes in, and later aggregated into analytics tables in Redshift.

    * If the pipelines were run on a daily basis by 7am.

    For that reason Airflow implementation could be a good choice to set the data pipeline execution hourly or daily, and implement data quality checks in case something fails send mails to be aware of the pipeline performance.
    
    * If the database needed to be accessed by 100+ people.

    While storing the DB in Redshift or another warehouse we can grant those access and set the cluster to handle that ammount of requests.