import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES FROM dwh.cgf

IMMIGRATIONS = config.get("S3","IMMIGRATIONS")
AIRPORTS = config.get("S3", "AIRPORTS")
DEMOGRAPHICS = config.get("S3", "DEMOGRAPHICS")
TEMPERATURES = config.get("S3", "TEMPERATURES")

IAM_ROLE = config.get("IAM_ROLE","ARN")

# Drop tables

drop_airports = "DROP TABLE IF EXISTS airports;"
drop_demographics = "DROP TABLE IF EXISTS demographics;"
drop_immigrations = "DROP TABLE IF EXISTS immigrations;"
drop_temperatures = "DROP TABLE IF EXISTS temperatures;"

# Create tables
                         
create_immigrations = """CREATE TABLE IF NOT EXISTS immigrations(
                        imm_idx                 INT,
                        cicid                   FLOAT PRIMARY KEY,
                        i94yr                   FLOAT,
                        i94mon                  FLOAT,
                        i94cit                  FLOAT,
                        i94res                  FLOAT,
                        i94port                 CHAR(3),
                        arrdate                 FLOAT,
                        i94mode                 FLOAT,
                        i94addr                 VARCHAR,
                        depdate                 FLOAT,
                        i94bir                  FLOAT,
                        i94visa                 FLOAT,
                        dtadfile                VARCHAR,
                        matflag                 CHAR(1),
                        biryear                 FLOAT,
                        dtaddto                 VARCHAR,
                        gender                  CHAR(1),
                        airline                 VARCHAR,
                        admnum                  FLOAT,
                        fltno                   VARCHAR,
                        visatype                VARCHAR
);"""


create_airports = """CREATE TABLE IF NOT EXISTS airports(
                        air_idx                 INT,
                        ident                   VARCHAR,
                        type                    VARCHAR,
                        name                    VARCHAR,
                        elevation_ft            FLOAT,
                        continent               VARCHAR,
                        iso_region              VARCHAR,
                        municipality            VARCHAR,
                        coordinates             VARCHAR
                        );"""

create_demographics = """CREATE TABLE IF NOT EXISTS demographics(
                        dem_idx                 INT,
                        city                    VARCHAR,
                        state                   VARCHAR,
                        median_age              FLOAT,
                        male_population         INT,
                        female_population       INT,
                        total_population        INT,
                        number_of_veterans      INT,
                        foreign_born            INT,
                        average_household_size  FLOAT,
                        state_code              CHAR(2),
                        race                    VARCHAR,
                        count                   INT
);"""

create_temperatures = """CREATE TABLE IF NOT EXISTS temperatures(
                        temp_idx                       INT,
                        dt                             VARCHAR,
                        AverageTemperature             FLOAT,
                        AverageTemperatureUncertainty  FLOAT,
                        City                           VARCHAR,
                        Country                        VARCHAR,
                        Latitude                       VARCHAR,
                        Longitude                      VARCHAR
);"""

# Copy to staging tables

immigrations_copy = (""" 
                copy immigrations from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(IMMIGRATIONS, IAM_ROLE)

airports_copy = (""" 
                copy airports from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(AIRPORTS, IAM_ROLE)

demographics_copy = (""" 
                copy demographics from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(DEMOGRAPHICS, IAM_ROLE)

temperatures_copy = (""" 
                copy temperatures from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(TEMPERATURES, IAM_ROLE)

# Insert data

drop_table_queries = [drop_immigrations, drop_airports, drop_demographics, drop_temperatures]
create_table_queries = [create_immigrations, create_airports, create_demographics, create_temperatures]
copy_table_queries = [immigrations_copy, airports_copy, demographics_copy, temperatures_copy]
tables = ["immigrations", "airports", "demographics", "temperatures"]