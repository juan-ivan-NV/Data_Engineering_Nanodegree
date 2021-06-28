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

## Directory overview

* capstone_project_notebook.ipynb

* sql_queries.py

* README.py

► plugins

    * __inint__.py

    ► helpers

        * __init__.py

        * table_configs.py

    ► operators

        * __inint__.py

        * copy_redshift.py

        * data_quality.py

        * sas_value_redshift.py

► dags

    * etl_dag.py

## Data exploration.

This stage is to use PySpark and look for the data frame informations, such as nulls, data frame dimentions, and types of data.

After taking a look a the data frames it comes the desition to keep usefull columns and get rid of useless columns

## Data selection and cleaning.



## Database creation to redshift.



### Schema.



## Consuming data (some analitics queries).






