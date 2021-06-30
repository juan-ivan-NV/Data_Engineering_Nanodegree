# Drop tables

drop_airports = "DROP TABLE IF EXISTS airports;"
drop_demographics = "DROP TABLE IF EXISTS demographics;"
drop_immigrations = "DROP TABLE IF EXISTS immigrations;"
drop_temperatures = "DROP TABLE IF EXISTS temperatures;"

# Create tables

create_immigrations = """CREATE TABLE IF NOT EXISTS immigrations(
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
                        city                    VARCAHR,
                        state                   VARCHAR,
                        median_age              FLOAT,
                        male_population         INT,
                        female_population       INT,
                        total_population        INT,
                        number_of_veterans      INT,
                        foreign_born            INT,
                        average_household_size  FLOAT,
                        state_code              CHAR(2),
                        Race                    VARCHAR,
                        Count                   INT
);"""

create_temperatures = """CREATE TABLE IF NOT EXISTS temperatures(
                        dt                             DATE,
                        AverageTemperature             FLOAT,
                        AverageTemperatureUncertainty  FLOAT,
                        City                           VARCHAR,
                        Country                        VARCHAR,
                        Latitude                       VARCHAR,
                        Longitude                      VARCHAR
);"""

# Insert data