# Drop tables

drop_airports = "DROP TABLE IF EXISTS airports;"
drop_demographics = "DROP TABLE IF EXISTS demographics;"
drop_immigrations = "DROP TABLE IF EXISTS immigrations;"
drop_temperatures = "DROP TABLE IF EXISTS temperatures;"

# Create tables

create_airports = """CREATE TABLE IF NOT EXISTS airports(
                        iata_code              VARCHAR PRIMARY KEY,
                        name                   VARCHAR,
                        type                   VARCHAR,
                        local_code             VARCHAR,
                        coordinates            VARCHAR,
                        city                   VARCHAR,
                        elevation_ft           VARCHAR,
                        continent              VARCHAR,
                        iso_country            VARCAHR,
                        iso_region             VARCHAR,
                        municipality           VARCHAR,
                        gps_code               VARCHAR
                        );"""

create_demographics = """CREATE TABLE IF NOT EXISTS demographics(
                        city                   VARCAHR,
                        state                  VARCHAR,
                        media_age              FLOAT,
                        male_population        INT,
                        female_populaiton      INT,
                        total_population       INT,
                        num_veterans           INT,
                        foreign_born           INT,
                        average_household_size FLOAT,
                        state_code             VARCHAR(2),
                        race                   VARCHAR,
                        count                  INT
);"""

create_immigrations = """CREATE TABLE IF NOT EXISTS immigrations(
                        cicid                  FLOAT PRIMARY KEY,
                        year                   FLOAT,
                        month                  FLOAT,
                        cit                    FLOAT,
                        res                    FLOAT,
                        iata                   VARCHAR(3),
                        arrdate                FLOAT,
                        mode                   FLOAT,
                        addr                   VARCHAR,
                        depdate                FLOAT,
                        bir                    FLOAT,
                        visa                   FLOAT,
                        count                  FLOAT,
                        dtadfile               VARCHAR,
                        entdepa                VARCHAR(1),
                        entdepd                VARCHAR(1),
                        matflag                VARCHAR(1),
                        biryear                FLOAT,
                        dtaddto                VARCHAR,
                        gender                 VARCHAR(1),
                        airline                VARCHAR,
                        admnum                 FLOAT,
                        fltno                  VARCHAR,
                        visatype               VARCHAR
);"""

create_temperatures = """CREATE TABLE IF NOT EXISTS temperatures(
                        timestamp                       DATE,
                        average_temperature             FLOAT,
                        average_temperature_uncertainty FLOAT,
                        city                            VARCHAR,
                        country                         VARCHAR,
                        latitude                        VARCHAR,
                        longitude                       VARCHAR
);"""

# Insert data

drop_table_queries = [drop_airports, drop_demographics, drop_immigrations, drop_temperatures]
create_table_queries = [create_airports, create_demographics, create_immigrations, create_temperatures]

