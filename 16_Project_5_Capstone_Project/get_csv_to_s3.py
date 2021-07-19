from io import StringIO # python3; python2: BytesIO 
import boto3
import configparser
import pandas as pd
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

bucket = 'my_bucket_name' # already created on S3

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col

spark = SparkSession.builder.getOrCreate()
print("Completed, ran successfully")


pd.set_option('max_columns', None)


# Function to drop columns
def drop_cols(df, *columns_to_drop):
    """
    This functions deletes the specified columns within the data frame.
    Inputs:
        DF ► the dataframe
        *columns_to_drop ► columns to drop
    """
    
    drop = columns_to_drop[0]
    print("Deleting columns ",drop)
    df = df.drop(*drop)
    
    return df


# Function to drop rows with nulls
def delete_nulls(df, columns_w_nulls):
    """
    Function to delete null values in certain columns
    Inputs:
        df ► the data frame
        columns_w_nulls ► columns choosen to delete their null rows
    """
    
    return df.na.drop(subset = columns_w_nulls)

# SAS_values_tables
def sas_value_parser(value, columns):
    """Parses SAS Program file to return value as pandas dataframe
    Args:
        value (str): sas value to extract.
        columns (list): list of 2 containing column names.
    """
    
    file = 'I94_SAS_Labels_Descriptions.SAS'
    file_string = ''
    
    with open(file) as f:
        file_string = f.read()
    
    file_string = file_string[file_string.index(value):]
    file_string = file_string[:file_string.index(';')]
    
    line_list = file_string.split('\n')[1:]
    codes = []
    values = []
    
    for line in line_list:
        
        if '=' in line:
            code, val = line.split('=')
            code = code.strip()
            val = val.strip()

            if code[0] == "'":
                code = code[1:-1]

            if val[0] == "'":
                val = val[1:-1]

            codes.append(code)
            values.append(val)
        
    df = pd.DataFrame(list(zip(codes, values)), columns=columns)
    return spark.createDataFrame(df)
    

# Function to upload csv files to S3
def csv_s3(key_id, secret_key, df, file_name, s3_path):
    """
    Function to upload CSV files to AWS S3
    Input:
        [Key_id, secret_key] ► AWS credentials
        df ► the data frame
        file_name ► the name of the csv file
        s3_path ► name of the bucket
    """
    
    if df.count() > 500000:
        csv_buffer = StringIO()
        df.limit(500000).toPandas().to_csv(csv_buffer)
        s3_resource = boto3.resource('s3',
        aws_access_key_id="xxxxx",
        aws_secret_access_key = "xxxxx")
        s3_resource.Object(s3_path, file_name).put(Body=csv_buffer.getvalue())
    
    else:
        csv_buffer = StringIO()
        df.toPandas().to_csv(csv_buffer)
        s3_resource = boto3.resource('s3',
        aws_access_key_id="xxxxx",
        aws_secret_access_key = "xxxxxxxx")
        s3_resource.Object(s3_path, file_name).put(Body=csv_buffer.getvalue())

    print("{} successfully submitted to {}".format(file_name, s3_path))

    
def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    path = "capstoneprojectde"
    key_id = config.get("CREDENTIALS", "AWS_ACCESS_KEY_ID")
    secret_key = config.get("CREDENTIALS", "AWS_SECRET_ACCESS_KEY")
    
    print("--------Working on immigrations file--------►")
    immigrations_df_spark = spark.read.load('./sas_data')
    immigrations_df_spark = drop_cols(immigrations_df_spark, ['visapost', 'occup', 'entdepu', 'insnum', 'entdepa', 'entdepd', 'entdepd', 'count', 'adnum'])
    csv_s3(key_id, secret_key, immigrations_df_spark, 'immigrations_data.csv', path)
    
    print("--------Working on i94_residence file--------►")
    i94_residence = sas_value_parser('i94cntyl', ['i94cit_res', 'country'])
    csv_s3(key_id, secret_key, i94_residence, 'i94_residence.csv', path)
    
    print("--------Working on i94_port_of_admission file--------►")
    i94_port_of_admission = sas_value_parser('i94prtl', ['i94port', 'port'])
    csv_s3(key_id, secret_key, i94_port_of_admission, 'i94_port_of_admission.csv', path)
    
    print("--------Working on i94_usa_state_arrival file--------►")
    i94_usa_state_arrival = sas_value_parser('i94addrl', ['i94addr', 'state'])
    csv_s3(key_id, secret_key, i94_usa_state_arrival, 'i94_usa_state_arrival.csv', path)
    
    print("--------Working on demographics_data file--------►")
    demographics_sp_df = spark.read.option("delimiter",";").option("header", True).csv('us-cities-demographics.csv')
    csv_s3(key_id, secret_key, demographics_sp_df, 'demographics_data.csv', path)
    
    print("--------Working on Temperature_data file--------►")
    temp_sp_df = spark.read.option("header", True).csv('../../data2/GlobalLandTemperaturesByCity.csv')
    temp_sp_df = delete_nulls(temp_sp_df, ["AverageTemperature","AverageTemperatureUncertainty"])
    csv_s3(key_id, secret_key, temp_sp_df, 'Temperature_data.csv', path)
    
if __name__ == "__main__":
    main()
    