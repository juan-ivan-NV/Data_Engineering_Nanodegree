import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES FROM dwh.cgf

IMMIGRATIONS = config.get("S3","IMMIGRATIONS")
AIRPORTS = config.get("S3", "AIRPORTS")
DEMOGRAPHICS = config.get("S3", "DEMOGRAPHICS")
TEMPERATURES = config.get("S3", "TEMPERATURES")
I94_RESIDENCE = config.get("S3", "I94_RESIDENCE")
I94_PORT_OF_ADMISSION = config.get("S3","I94_PORT_OF_ADMISSION")
I94_USA_STATE_ARRIVAL = config.get("S3", "I94_USA_STATE_ARRIVAL")

IAM_ROLE = config.get("IAM_ROLE","ARN")

# Drop tables

drop_airports = "DROP TABLE IF EXISTS airports;"
drop_demographics = "DROP TABLE IF EXISTS demographics;"
drop_immigrations = "DROP TABLE IF EXISTS immigrations;"
drop_temperatures = "DROP TABLE IF EXISTS temperatures;"
drop_i94_residence = "DROP TABLE IF EXISTS i94_residence;"
drop_i94_port_of_admission = "DROP TABLE IF EXISTS i94_port_of_admission;"
drop_i94_usa_state_arrival = "DROP TABLE IF EXISTS i94_usa_state_arrival;"

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

create_i94_residence = """CREATE TABLE IF NOT EXISTS i94_residence(
                        res_idx                        INT,
                        i94cit_res                     FLOAT,
                        country                        VARCHAR
);"""

create_i94_port_of_admission = """CREATE TABLE IF NOT EXISTS i94_port_of_admission(
                        por_idx                        INT,
                        i94port                        CHAR(3),
                        port                           VARCHAR
);"""

create_i94_usa_state_arrival = """CREATE TABLE IF NOT EXISTS i94_usa_state_arrival(
                        arr_idx                        INT,
                        i94addr                        VARCHAR,
                        state                          VARCHAR
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

i94_residence_copy = (""" 
                copy i94_residence from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(I94_RESIDENCE, IAM_ROLE)

i94_port_of_admission_copy = (""" 
                copy i94_port_of_admission from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(I94_PORT_OF_ADMISSION, IAM_ROLE)

i94_usa_state_arrival_copy = (""" 
                copy i94_usa_state_arrival from {}
                credentials 'aws_iam_role={}'
                region 'us-west-2'
                CSV
                IGNOREHEADER 1
                DELIMITER ',';
""").format(I94_USA_STATE_ARRIVAL, IAM_ROLE)

# Insert data

drop_table_queries = [drop_immigrations, drop_demographics, drop_temperatures, drop_i94_residence, drop_i94_port_of_admission, drop_i94_usa_state_arrival]
create_table_queries = [create_immigrations, create_demographics, create_temperatures, create_i94_residence, create_i94_port_of_admission, create_i94_usa_state_arrival]
copy_table_queries = [immigrations_copy, demographics_copy, temperatures_copy, i94_residence_copy, i94_port_of_admission_copy, i94_usa_state_arrival_copy]
tables = ["immigrations", "demographics", "temperatures", "i94_residence", "i94_port_of_admission", "i94_usa_state_arrival"]
tables_keys = {"immigrations":"cicid", "demographics" : "city", "temperatures" : "dt", "i94_residence" : "i94cit_res", "i94_port_of_admission" : "i94port", "i94_usa_state_arrival" : "i94addr"}