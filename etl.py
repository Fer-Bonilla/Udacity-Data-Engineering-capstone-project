# -*- coding: utf-8 -*-
"""
    This script implements the ETL pipeline to execute the process to read csv data from BTC and ETH cyptocurrencies and create the spark datalake    
        
    The pipeline is implemented using pyspark with datafranes and parquet files
        
"""

import pandas as pd
import re
import boto3
import zipfile
from pyspark.sql import SparkSession
import os
import glob
import configparser
from datetime import datetime, timedelta, date
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col, monotonically_increasing_id, to_date, to_timestamp, isnan, when, count
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, minute
import matplotlib.pyplot as plt
import seaborn as sns

def create_spark_session():
    
    """
        The function create_spark_session create the object session for the apache spark service
        Parameters:
            None
        Returns:
            spark session object
        Note:
            None
    """   
    
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark



def process_setup_variables(config):
    
    """
        The function process_setup_variables load the configuration file and create the gloal configuration variables
        Parameters:
            config_all : Object tha contains all configuration values from dl.cfg file
        Returns:
            spark session object
        Note:
            None
    """

    # NOTE: Use these if using AWS S3 as a storage
    INPUT_DATA_AWS                = config['AWS']['INPUT_DATA_AWS']
    OUTPUT_DATA_AWS               = config['AWS']['OUTPUT_DATA_AWS']

    # NOTE: Use these if using local storage
    INPUT_DATA_LOCAL              = config['LOCAL']['INPUT_DATA_LOCAL']
    OUTPUT_DATA_LOCAL             = config['LOCAL']['OUTPUT_DATA_LOCAL']

    # Common configuration parameters
    DATA_LOCATION                 = config['COMMON']['DATA_LOCATION']
    DATA_STORAGE                  = config['COMMON']['DATA_STORAGE']
    INPUT_DATA_BTC_DIRECTORY      = config['COMMON']['INPUT_DATA_BTC_DIRECTORY']
    INPUT_DATA_BTC_ZIP_FILENAME   = config['COMMON']['INPUT_DATA_BTC_ZIP_FILENAME']
    INPUT_DATA_BTC_FILENAME       = config['COMMON']['INPUT_DATA_BTC_FILENAME']
    INPUT_DATA_ETH_DIRECTORY      = config['COMMON']['INPUT_DATA_ETH_DIRECTORY']
    INPUT_DATA_ETH_ZIP_FILENAME   = config['COMMON']['INPUT_DATA_ETH_ZIP_FILENAME']
    INPUT_DATA_ETH_FILENAME       = config['COMMON']['INPUT_DATA_ETH_FILENAME']
    OUTPUT_DATA_BTC_FILENAME      = config['COMMON']['OUTPUT_DATA_BTC_FILENAME']
    OUTPUT_DATA_ETH_FILENAME      = config['COMMON']['OUTPUT_DATA_ETH_FILENAME']
    OUTPUT_BTC_TABLE_FILENAME     = config['COMMON']['OUTPUT_BTC_TABLE_FILENAME']
    OUTPUT_ETH_TABLE_FILENAME     = config['COMMON']['OUTPUT_ETH_TABLE_FILENAME']
    OUTPUT_CRYPTO_TABLE_FILENAME  = config['COMMON']['OUTPUT_CRYPTO_TABLE_FILENAME']
    
    PATHS = {}
    
    # Set global configuration variables
    if DATA_LOCATION == "local":
        PATHS["input_data"]          = INPUT_DATA_LOCAL
        PATHS["output_data"]         = OUTPUT_DATA_LOCAL

    elif DATA_LOCATION == "aws":
        PATHS["input_data"]           = INPUT_DATA_AWS
        PATHS["output_data"]          = OUTPUT_DATA_AWS

    elif DATA_STORAGE == "parquet":
        PATHS["data_storage"]         = DATA_STORAGE

    # load variables for BTC data
    PATHS["btc_data_directory"]       = INPUT_DATA_BTC_DIRECTORY
    PATHS["btc_zip_filename"]         = INPUT_DATA_BTC_ZIP_FILENAME    
    PATHS["btc_filename"]             = OUTPUT_DATA_BTC_FILENAME
    PATHS["btc_table_filename"]       = OUTPUT_BTC_TABLE_FILENAME

    # load variables for ETH data
    PATHS["eth_data_directory"]       = INPUT_DATA_ETH_DIRECTORY
    PATHS["eth_zip_filename"]         = INPUT_DATA_ETH_ZIP_FILENAME    
    PATHS["eth_filename"]             = OUTPUT_DATA_ETH_FILENAME
    PATHS["eth_table_filename"]       = OUTPUT_ETH_TABLE_FILENAME

    # general variables
    PATHS["crypto_timeseries_table"] = OUTPUT_CRYPTO_TABLE_FILENAME  

    return PATHS



def process_btc_input_data(spark, PATHS):
    
    """
        The function process_btc_input_data read all csv files from input data directory 
        and write the parquet file with all the raw data
        
        Parameters:
            spark (obj): 
                spark object session
            input_data (str): 
                Path to the btc data files
            output_data (str): 
                Path to write the parquet files
                
        Returns:
            None
            
        Note:
             The function write direct to S3 bucket songs and artists tables in parket format.             
    """   
    
    # Read csv files
    btc_data_staging = spark.read.options(header='True', inferSchema='True').csv(PATHS["btc_data_directory"])

    # Rename btc input file columns
    btc_data_staging_temp = btc_data_staging.withColumnRenamed("Volume_(BTC)", "Volume_BTC") \
                                            .withColumnRenamed("Volume_(Currency)", "Volume_Currency")

    # Write to parquet file
    btc_data_staging_temp.write.mode("overwrite").parquet(PATHS["output_data"]+PATHS["btc_filename"])
    
    print('PROCESS: process_btc_input_data is done')


    
def process_eth_input_data(spark, PATHS):
    
    """
        The function process_eth_input_data read all csv files from input data directory 
        and write the parquet file with all the raw data
        
        Parameters:
            spark (obj): 
                spark object session
            PATHS (str): 
                Global variable data with names and routes to reed and write parquet files
                
        Returns:
            None
            
        Note:
             The function write the parquet file with the staging table info           
    """   
    
    # Read csv files

    eth_data_staging = spark.read.options(header='True', inferSchema='True').csv(PATHS['eth_data_directory'])
    
    # Write to parquet file
    eth_data_staging.write.mode("overwrite").parquet(PATHS["output_data"]+PATHS["eth_filename"])  
    
    print('PROCESS: process_eth_input_data is done')

    
def btc_timseries_table_generation(spark, PATHS):
    """
        The function btc_timseries_table_generation read the data from staging_btc 
        and write the parquet file with the filtered table
        
        Parameters:
            spark (obj): 
                spark object session
            PATHS (str): 
                Global variable data with names and routes to reed and write parquet files
                
        Returns:
            None
            
        Note: The function write the parquet file with the timeseries info

    """

    # Read the data from staging_btc parquet file
    btc_data_staging = spark.read.parquet(PATHS['output_data']+PATHS['btc_filename'])
    
    # Drop null values      
    btc_data_staging_temp = btc_data_staging.na.drop()

    # Format date into year, month, day, hour and minute columns      
    btc_data_staging_temp = btc_data_staging_temp.withColumn('year',year(to_timestamp('Timestamp')))
    btc_data_staging_temp = btc_data_staging_temp.withColumn('month',month(to_timestamp('Timestamp')))
    btc_data_staging_temp = btc_data_staging_temp.withColumn('day',dayofmonth(to_timestamp('Timestamp')))
    btc_data_staging_temp = btc_data_staging_temp.withColumn('hour',hour(to_timestamp('Timestamp')))
    btc_data_staging_temp = btc_data_staging_temp.withColumn('minute',minute(to_timestamp('Timestamp')))          
          
    # Create the btc_timeseries table
    btc_data_staging_temp.createOrReplaceTempView("btc_timeseries")
    btc_timeseries_table = spark.sql("""
        SELECT  DISTINCT Timestamp    AS timestamp,
                         year         AS year, 
                         month        AS month, 
                         day          AS day, 
                         hour         AS hour, 
                         minute       AS minute,
                         Open         AS btc_open, 
                         High         AS btc_high, 
                         Low          AS btc_low,
                         Volume_BTC   AS btc_volume                     
        FROM btc_timeseries
        ORDER BY year, month, day, hour, minute
    """)
          
    btc_timeseries_table.write.mode("overwrite").parquet(PATHS['output_data']+PATHS['btc_table_filename'])
    
    print('PROCESS: btc_timseries_table_generation is done')

    
def eth_timseries_table_generation(spark, PATHS):
    """
        The function eth_timseries_table_generation read the data from staging_eth 
        and write the parquet file with the filtered table
        
        Parameters:
            spark (obj): 
                spark object session
            PATHS (str): 
                Global variable data with names and routes to reed and write parquet files
                
        Returns:
            None
            
        Note: The function write the parquet file with the timeseries info

    """

    # Read the data from staging_btc parquet file
    eth_data_staging_temp = spark.read.parquet(PATHS['output_data']+PATHS['eth_filename'])

    # Format date into year, month, day, hour and minute columns      
    eth_data_staging_temp = eth_data_staging_temp.withColumn('year',year(to_timestamp('Timestamp')))
    eth_data_staging_temp = eth_data_staging_temp.withColumn('month',month(to_timestamp('Timestamp')))
    eth_data_staging_temp = eth_data_staging_temp.withColumn('day',dayofmonth(to_timestamp('Timestamp')))
    eth_data_staging_temp = eth_data_staging_temp.withColumn('hour',hour(to_timestamp('Timestamp')))
    eth_data_staging_temp = eth_data_staging_temp.withColumn('minute',minute(to_timestamp('Timestamp')))      
          
    # Create the btc_timeseries table
    eth_data_staging_temp.createOrReplaceTempView("eth_timeseries")
    eth_timeseries_table = spark.sql("""
        SELECT  DISTINCT timestamp    AS timestamp,
                         year         AS year, 
                         month        AS month, 
                         day          AS day, 
                         hour         AS hour, 
                         minute       AS minute,
                         open         AS eth_open, 
                         high         AS eth_high, 
                         low          AS eth_low,
                         volume       AS eth_volume                     
        FROM eth_timeseries
        ORDER BY year, month, day, hour, minute
    """)
          
    eth_timeseries_table.write.mode("overwrite").parquet(PATHS['output_data']+PATHS['eth_table_filename'])
          
    print('PROCESS: eth_timseries_table_generation is done')

    
def crypto_timseries_table_generation(spark, PATHS):
    """
        The function crypto_timseries_table_generation read the data from btc_timeseries and 
        eth_timeseries parquet file, execute the join and write the parquet file with the filtered table
        
        Parameters:
            spark (obj): 
                spark object session
            PATHS (str): 
                Global variable data with names and routes to reed and write parquet files
                
        Returns:
            None
            
        Note: The function write the parquet file with the crypto timeseries info

    """

    # Read the data from btc_timserie parquet file
    btc_timserie_table = spark.read.parquet(PATHS['output_data']+PATHS['btc_table_filename'])

    # Read the data from eth_timserie parquet file
    eth_timserie_table = spark.read.parquet(PATHS['output_data']+PATHS['eth_table_filename'])
 
    # Join BTC and ETH tables by year, month, day, hour adn minute keys
    crypto_timeseries_spark = btc_timserie_table.alias('b').join(eth_timserie_table.alias('e'), on=[
                                                btc_timserie_table.year ==  eth_timserie_table.year,
                                                btc_timserie_table.month ==  eth_timserie_table.month,
                                                btc_timserie_table.day ==  eth_timserie_table.day,
                                                btc_timserie_table.hour ==  eth_timserie_table.hour,
                                                btc_timserie_table.minute ==  eth_timserie_table.minute
                                                ]).select("b.year", 
                                                          "b.month",
                                                          "b.day",
                                                          "b.hour",
                                                          "b.minute",
                                                          "b.btc_open",
                                                          "b.btc_high",
                                                          "b.btc_low",
                                                          "b.btc_volume",                                
                                                          "e.eth_open",
                                                          "e.eth_high",
                                                          "e.eth_low",
                                                          "e.eth_volume").sort("year","month","day","hour","minute")    
          
    # Write crypto_timeseries_table to parquet file:
    crypto_timeseries_spark.write.mode("overwrite").parquet(PATHS['output_data']+PATHS['crypto_timeseries_table'])          
          
    print('PROCESS: crypto_timseries_table_generation is done')          

            
def check_data_quality(spark):
    """
        The function check_data_quality read the data from btc_timeseries and 
        eth_timeseries parquet file, execute the join and write the parquet file with the filtered table
        
        Parameters:
            spark (obj): 
                spark object session
            PATHS (str): 
                Global variable data with names and routes to reed and write parquet files
                
        Returns:
            None
            
        Note: The function write the parquet file with the crypto timeseries info

    """
   
    # verify table is not empty and key don' have null values
    btc_data_staging_check = spark.read.parquet(PATHS["output_data"]+PATHS["btc_filename"])
    btc_data_staging_check.select(count(col('timestamp'))).show()
    btc_data_staging_check.select(count(when(isnan('timestamp') | col('timestamp').isNull(), 'timestamp'))).show()
    
    # verify table is not empty and key don' have null values
    eth_data_staging_check = spark.read.parquet(PATHS["output_data"]+PATHS["eth_filename"])
    eth_data_staging_check.select(count(col('timestamp'))).show()
    eth_data_staging_check.select(count(when(col('timestamp').isNull(), 'timestamp'))).show()


    # verify table is not empty and key don' have null values
    btc_data_series_check = spark.read.parquet(PATHS['output_data']+PATHS['btc_table_filename'])
    btc_data_series_check.select(count(col('timestamp'))).show()
    btc_data_series_check.select(count(when(col('timestamp').isNull(), 'timestamp'))).show()

    # verify table is not empty and key don' have null values
    eth_data_series_check = spark.read.parquet(PATHS['output_data']+PATHS['eth_table_filename'])
    eth_data_series_check.select(count(col('timestamp'))).show()
    eth_data_series_check.select(count(when(col('timestamp').isNull(), 'timestamp'))).show()


    # verify table is not empty and key don' have null values
    crypto_timeserie_check = spark.read.parquet(PATHS['output_data']+PATHS['crypto_timeseries_table'])
    crypto_timeserie_check.select(count(col('year'))).show()
    crypto_timeserie_check.select(count(when(col('year').isNull(), 'year'))).show()


def main():
    
    # Prepare configs for the pipeline.
    config_all = configparser.ConfigParser()
    config_all.read('dl.cfg')
    
    # Load the keys for AWS service (Only if AWS Spark cluister is used)
    os.environ['AWS_ACCESS_KEY_ID']=config_all['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config_all['AWS']['AWS_SECRET_ACCESS_KEY']

    # Load JAVA, spark and hadoop configuration path directories
#     os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#     os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
#     os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
#     os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    
    
    # Load the global variables
    PATHS = process_setup_variables(config_all)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read the csv Bitcoin data and create the staging_btc table and write the parquet file
    process_btc_input_data(spark, PATHS)

    # Read the csv Ethereum data and create the staging_btc table and write the parquet file
    process_eth_input_data(spark, PATHS)
    
    # Read the staging_btc data and create the btc_timserie table and write the parquet file
    btc_timseries_table_generation(spark, PATHS)
    
    # Read the staging_eth data and create the eth_timserie table and write the parquet file
    eth_timseries_table_generation(spark, PATHS)
    
    # Read data from btc_timserie and eth_timserie and create the join dataframe with data for both cryptocurrencies
    crypto_timseries_table_generation(spark, PATHS)
    
    # Execute data qualuty process
    check_data_quality(spark)
    
if __name__ == "__main__":
    main()
