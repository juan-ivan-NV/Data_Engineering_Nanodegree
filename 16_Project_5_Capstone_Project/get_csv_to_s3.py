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
    df.printSchema()
    df = df.drop(*columns_to_drop)
    df.printSchema()
    
    return df

# Function to drop rows with nulls



# Function to upload csv files to S3

def csv_s3(df, file_name, s3_path):
    
    if df.count() > 1000000:
        csv_buffer = StringIO()
        df.limit(1000000).toPandas().to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
    
    else:
        csv_buffer = StringIO()
        df.toPandas().to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())

    print("{} successfully submitted to {}".format(file_name, s3_path))
def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    path = *config['BUCKET'].values()
    
    # immigrations df
    immigrations_df_spark = spark.read.load('./sas_data')
    immigrations_df_spark = drop_cols(immigrations_df_spark, ['visapost', 'occup', 'entdepu', 'insnum', 'entdepa', 'entdepd', 'entdepd', 'count', 'adnum'])
    # 'immigrations_data.csv'
    # 



