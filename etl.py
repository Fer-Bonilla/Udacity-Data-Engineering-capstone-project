# -*- coding: utf-8 -*-
"""
    This script implements the ETL pipeline to execute the process using amazon AWS services and apache Spark
    
    Staging tables:
        - staging_events - Load the raw data from log events json files artist
        auth, firstName, gender, itemInSession,    lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
        - staging_songs - num_songs artist_id artist_latitude artist_longitude artist_location artist_name song_id title duration year
       
    Dimension tables:
        - users - users in the app: user_id, first_name, last_name, gender, level
        - songs - songs in music database: song_id, title, artist_id, year, duration
        - artists - artists in music database: artist_id, name, location, latitude, longitude
        - time - timestamps of records in songplays: start_time, hour, day, week, month, year, weekday
    
    Fact Table:
        - songplays - records in log data associated with song plays.
        
    The pipeline is implemented using dataframes loading data from Postgres database with the psycopg2 connector.        
        
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
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
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



def process_setup_variables(config_all):
    
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
    INPUT_DATA_AWS              = config['AWS']['INPUT_DATA_AWS']
    OUTPUT_DATA_AWS             = config['AWS']['OUTPUT_DATA_AWS']

    # NOTE: Use these if using local storage
    INPUT_DATA_LOCAL            = config['LOCAL']['INPUT_DATA_LOCAL']
    OUTPUT_DATA_LOCAL           = config['LOCAL']['OUTPUT_DATA_LOCAL']

    # Common configuration parameters
    DATA_LOCATION               = config['COMMON']['DATA_LOCATION']
    DATA_STORAGE                = config['COMMON']['DATA_STORAGE']
    INPUT_DATA_BTC_DIRECTORY    = config['COMMON']['INPUT_DATA_BTC_DIRECTORY']
    INPUT_DATA_BTC_ZIP_FILENAME = config['COMMON']['INPUT_DATA_BTC_ZIP_FILENAME']
    INPUT_DATA_BTC_FILENAME     = config['COMMON']['INPUT_DATA_BTC_FILENAME']
    INPUT_DATA_ETH_DIRECTORY    = config['COMMON']['INPUT_DATA_ETH_DIRECTORY']
    INPUT_DATA_ETH_ZIP_FILENAME = config['COMMON']['INPUT_DATA_ETH_ZIP_FILENAME']
    INPUT_DATA_ETH_FILENAME     = config['COMMON']['INPUT_DATA_ETH_FILENAME']
    OUTPUT_DATA_BTC_FILENAME    = config['COMMON']['OUTPUT_DATA_BTC_FILENAME']
    OUTPUT_DATA_ETH_FILENAME    = config['COMMON']['OUTPUT_DATA_ETH_FILENAME']
    
    PATHS = {}
    
    # Select the global variables
    if DATA_LOCATION == "local":
        PATHS["input_data"]        = INPUT_DATA_LOCAL
        PATHS["output_data"]       = OUTPUT_DATA_LOCAL

    elif DATA_LOCATION == "aws":
        PATHS["input_data"]        = INPUT_DATA_AWS
        PATHS["output_data"]       = OUTPUT_DATA_AWS

    elif DATA_STORAGE == "parquet":
        PATHS["data_storage"]      = DATA_STORAGE

    # load variables for BTC data
    PATHS["btc_data_directory"]    = INPUT_DATA_BTC_DIRECTORY
    PATHS["btc_zip_filename"]      = INPUT_DATA_BTC_ZIP_FILENAME    
    PATHS["btc_filename"]          = INPUT_DATA_BTC_FILENAME
    PATHS["btc_output_filename"]   = OUTPUT_DATA_BTC_FILENAME    

    # load variables for ETH data
    PATHS["eth_data_directory"]    = INPUT_DATA_ETH_DIRECTORY
    PATHS["eth_zip_filename"]      = INPUT_DATA_ETH_ZIP_FILENAME    
    PATHS["eth_filename"]          = INPUT_DATA_ETH_FILENAME
    PATHS["eth_output_filename"]   = OUTPUT_DATA_ETH_FILENAME    

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
    btc_data_staging_temp.write.mode("overwrite").parquet(PATHS["output_data"]+PATHS["btc_output_filename"])


    
def process_eth_input_data(spark, PATHS):
    
    """
        The function process_eth_input_data read all csv files from input data directory 
        and write the parquet file with all the raw data
        
        Parameters:
            spark (obj): 
                spark object session
            input_data (str): 
                Path to the eth data files
            output_data (str): 
                Path to write the parquet files
                
        Returns:
            None
            
        Note:
             The function write direct to S3 bucket songs and artists tables in parket format.             
    """   
    
    # Read csv files

    eth_data_staging = spark.read.options(header='True', inferSchema='True').csv(PATHS['eth_data_directory'])
    
    # Write to parquet file
    eth_data_staging.write.mode("overwrite").parquet(PATHS["output_data"]+PATHS["eth_output_filename"])  
    

    
def format_btc_staging_data(spark, PATHS, i94_df_spark, start_time):
    """Clean i94 data - fill-in empty/null values with "NA"s or 0s.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * i94_df_spark_clean -- clean Spark DataFrame.
    """
    start_local = datetime.now()
    print("Cleaning i94 data...")
    # Filling-in empty/null data with "NA"s or 0's
    i94_df_spark_clean = i94_df_spark\
        .na.fill({'i94mode': 0.0, 'i94addr': 'NA','depdate': 0.0, \
            'i94bir': 'NA', 'i94visa': 0.0, 'count': 0.0, \
            'dtadfile': 'NA', 'visapost': 'NA', 'occup': 'NA', \
            'entdepa': 'NA', 'entdepd': 'NA', 'entdepu': 'NA', \
            'matflag': 'NA', 'biryear': 0.0, 'dtaddto': 'NA', \
            'gender': 'NA', 'insnum': 'NA', 'airline': 'NA', \
            'admnum': 0.0, 'fltno': 'NA', 'visatype': 'NA'
            })
    print("Cleaning i94 data DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 data cleaning DONE in: {total_local}\n")

    return i94_df_spark_clean
    
    

def process_i94_data(spark, PATHS, filepath, start_time):
    """Load input data (i94) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * start_time        -- Datetime when the pipeline was started.
                            Used for name parquet files.

    Output:
    * i94_staging_table -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Processing i94 data ...")

    month, year = parse_year_and_month(filepath)
    print(f"YEAR+MONTH: {year} - {month}")

    # Read data to Spark
    i94_df_spark =spark.read\
                        .format('com.github.saurfang.sas.spark')\
                        .load(filepath)

    # Print schema and data snippet
    print("SCHEMA:")
    i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    i94_df_spark.show(2, truncate=False)

    # Write data to parquet file:
    i94_df_path = PATHS["output_data"] \
                    + "i94_staging" \
                    + "_" + year + "_" + month + "_" \
                    + ".parquet" \
                    + "_" + start_time
    print(f"OUTPUT: {i94_df_path}")
    print("Writing parquet files ...")
    i94_df_spark.write.mode("overwrite").parquet(i94_df_path)
    print("Writing i94 staging files DONE.\n")

    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark...")
    i94_df_spark = spark.read.parquet(i94_df_path)
    print("Reading parquet files back to Spark DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 data processing DONE in: {total_local}\n")

    return i94_df_spark

# --------------------------------------------------------
def process_i94_airport_data(spark, PATHS, start_time):
    """Load input data (i94 airports) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * start_time        -- Datetime when the pipeline was started.
                            Used for name parquet files.

    Output:
    * i94_airport_staging_table -- directory with parquet files
                                    stored in output data path.
    """
    start_local = datetime.now()
    print("Processing i94_airport data ...")
    # Read I94 Airport codes data from XLS:
    airport_codes_i94_df = pd.read_excel(PATHS["airport_codes_i94"], \
                                            header=0, index_col=0)
    # --------------------------------------------------------
    # Cleaning I94 Airport data first
    print("Cleaning I94 airport data...")
    ac = {  "i94port_clean": [],
            "i94_airport_name_clean": [],
            "i94_state_clean": []
        }
    codes = []
    names = []
    states = []
    for index, row in airport_codes_i94_df.iterrows():
        y = re.sub("'", "", index)
        x = re.sub("'", "", row[0])
        z = re.sub("'", "", row[0]).split(",")
        y = y.strip()
        z[0] = z[0].strip()

        if len(z) == 2:
            codes.append(y)
            names.append(z[0])
            z[1] = z[1].strip()
            states.append(z[1])
        else:
            codes.append(y)
            names.append(z[0])
            states.append("NaN")

    ac["i94port_clean"] = codes
    ac["i94_airport_name_clean"] = names
    ac["i94_state_clean"] = states

    airport_codes_i94_df_clean = pd.DataFrame.from_dict(ac)
    print("Cleaning I94 airport data DONE.")
    # --------------------------------------------------------
    # Writing clean data to CSV (might be needed at some point)
    print("Writing I94 airport data to CSV...")
    ac_path = PATHS["input_data"] + "/airport_codes_i94_clean.csv"
    airport_codes_i94_df_clean.to_csv(ac_path, sep=',')
    print("Writing I94 airport data to CSV DONE.")
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading I94 airport data to Spark...")
    airport_codes_i94_schema = t.StructType([
                    t.StructField("i94_port", t.StringType(), False),
                    t.StructField("i94_airport_name", t.StringType(), False),
                    t.StructField("i94_airport_state", t.StringType(), False)
                ])
    airport_codes_i94_df_spark = spark.createDataFrame(\
                            airport_codes_i94_df_clean, \
                            schema=airport_codes_i94_schema)
    # --------------------------------------------------------
    # Print schema and data snippet
    print("SCHEMA:")
    airport_codes_i94_df_spark.printSchema()
    print("DATA EXAMPLES:")
    airport_codes_i94_df_spark.show(2, truncate=False)
    # --------------------------------------------------------
    # Write data to parquet file:
    airport_codes_i94_df_path = PATHS["output_data"] \
                                + "airport_codes_i94_staging.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {airport_codes_i94_df_path}")
    print("Writing parquet files ...")
    airport_codes_i94_df_spark.write.mode("overwrite")\
                                .parquet(airport_codes_i94_df_path)
    print("Writing i94 airport staging files DONE.")

    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark")
    airport_codes_i94_df_spark = spark.read\
                                .parquet(airport_codes_i94_df_path)
    print("Reading parquet files back to Spark DONE.")

    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"I94 Airport code processing DONE in: {total_local}\n")

    return airport_codes_i94_df_spark


# --------------------------------------------------------
def process_iso_country_code_data(spark, PATHS, start_time):
    """Load input data (ISO-3166 Country Codes) from input path,
        read the data to Spark and
        store the data to parquet staging files.

    Keyword arguments:
    * spark                 -- reference to Spark session.
    * PATHS                 -- paths for input and output data.
    * start_time            -- Datetime when the pipeline was started.
                                Used for name parquet files.

    Output:
    * iso_country_codes_staging_table -- directory with parquet files
                                        stored in output data path.
    """
    start_local = datetime.now()
    print("Processing ISO-3166 Country Codes data ...")
    # Read I94 Country codes data from XLS:
    country_codes_iso_df = pd.read_csv(PATHS["country_codes_iso"], header=0)
    # --------------------------------------------------------
    # Writing clean data to CSV (might be needed at some point)
    print("Writing ISO-3166 Country Code data to CSV...")
    cc_path = PATHS["input_data"] + "/country_codes_iso_clean.csv"
    country_codes_iso_df.to_csv(cc_path, sep=',')
    print("Writing ISO-3166 Country Code data to CSV DONE.")
    print("Cleaning ISO-3166 Country Code data DONE.")
    # --------------------------------------------------------
    # Read data to Spark
    print("Reading ISO-3166 Country Code data to Spark...")
    country_codes_iso_schema = t.StructType([
                t.StructField("name", t.StringType(), False),
                t.StructField("alpha_2", t.StringType(), False),
                t.StructField("alpha_3", t.StringType(), False),
                t.StructField("country_code", t.StringType(), False),
                t.StructField("iso_3166_2", t.StringType(), False),
                t.StructField("region", t.StringType(), True),
                t.StructField("sub_region", t.StringType(), True),
                t.StructField("intermediate_region", t.StringType(), True),
                t.StructField("region_code", t.StringType(), True),
                t.StructField("sub_region_code", t.StringType(), True),
                t.StructField("intermediate_region_code", t.StringType(), True),
            ])
    country_codes_iso_df_spark = spark.createDataFrame(\
                                country_codes_iso_df, \
                                schema=country_codes_iso_schema)
    # --------------------------------------------------------
    # Print schema and data snippet
    print("SCHEMA:")
    country_codes_iso_df_spark.printSchema()
    # print("DATA EXAMPLES:")
    # #country_codes_iso_df_spark.show(2, truncate=False)
    # --------------------------------------------------------
    # Write ISO-3166 Country data to parquet file:
    country_codes_iso_df_path = PATHS["output_data"] \
                                + "country_codes_iso_staging.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {country_codes_iso_df_path}")
    print("Writing parquet files ...")
    country_codes_iso_df_spark.write.mode("overwrite")\
                                .parquet(country_codes_iso_df_path)
    print("Writing ISO-3166 Country Code staging files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    print("Reading parquet files back to Spark... ")
    country_codes_iso_df_spark = spark.read.\
                                 parquet(country_codes_iso_df_path)
    print("Reading parquet files back to Spark DONE.")
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"ISO-3166 Country Code processing DONE in: {total_local}\n")

    return country_codes_iso_df_spark

# --------------------------------------------------------


# --------------------------------------------------------
def process_admissions_data(spark, PATHS, i94_df_spark_clean, start_time):
    """Load input data (i94_clean),
        process the data to extract admissions table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark               -- reference to Spark session.
    * PATHS               -- paths for input and output data.
    * i94_df_spark_clean  -- cleaned i94 Spark dataframe.
    * start_str           -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * admissions_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating admissions_table...")
    # Create table + query
    i94_df_spark_clean.createOrReplaceTempView("admissions_table_DF")
    admissions_table = spark.sql("""
        SELECT  DISTINCT admnum   AS admission_nbr,
                         i94res   AS country_code,
                         i94bir   AS age,
                         i94visa  AS visa_code,
                         visatype AS visa_type,
                         gender   AS person_gender
        FROM admissions_table_DF
        ORDER BY country_code
    """)

    print("SCHEMA:")
    admissions_table.printSchema()
    #print("DATA EXAMPLES:")
    #admissions_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    admissions_table_path = PATHS["output_data"] \
                            + "admissions_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {admissions_table_path}")
    admissions_table.write.mode("overwrite")\
                        .parquet(admissions_table_path)
    print("Writing admissions_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    admissions_table_df = spark.read.parquet(admissions_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating admissions_table DONE in: {total_local}\n")

    return admissions_table_df

# --------------------------------------------------------
def process_countries_data( spark, \
                            PATHS, \
                            country_codes_i94_df_spark, \
                            country_codes_iso_df_spark, \
                            start_time):
    """Load input data (country_codes_clean),
        process the data to extract countries table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * country_codes_i94_df_spark -- cleaned i94 country code
                                    Spark dataframe
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * countries_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating countries_table...")

    country_codes_i94_df_spark_joined = country_codes_i94_df_spark\
        .join(country_codes_iso_df_spark, \
            (country_codes_i94_df_spark.iso_country_code == \
                    country_codes_iso_df_spark.country_code))

    # print("SCHEMA_joined_table (before new table): ")
    # country_codes_i94_df_spark_joined.printSchema()
    # country_codes_i94_df_spark_joined.show(20, truncate=False)

    # Create table + query
    country_codes_i94_df_spark_joined.createOrReplaceTempView("countries_table_DF")
    countries_table = spark.sql("""
        SELECT DISTINCT i94_cit          AS country_code,
                        i94_country_name AS country_name,
                        iso_country_code AS iso_ccode,
                        alpha_2          AS iso_alpha_2,
                        alpha_3          AS iso_alpha_3,
                        iso_3166_2       AS iso_3166_2_code,
                        name             AS iso_country_name,
                        region           AS iso_region,
                        sub_region       AS iso_sub_region,
                        region_code      AS iso_region_code,
                        sub_region_code  AS iso_sub_region_code
        FROM countries_table_DF          AS countries
        ORDER BY country_name
    """)

    print("SCHEMA:")
    countries_table.printSchema()
    # print("DATA EXAMPLES:")
    # countries_table.show(50, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    countries_table_path = PATHS["output_data"] \
                            + "countries_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {countries_table_path}")
    countries_table.write.mode("overwrite").parquet(countries_table_path)
    print("Writing DONE.")
    print("Writing countries_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    countries_table_df = spark.read.parquet(countries_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating countries_table DONE in: {total_local}\n")

    return countries_table_df

# --------------------------------------------------------
def process_airport_data(spark, PATHS, airport_codes_i94_df_spark, start_time):
    """Load input data (airport_codes_clean),
        process the data to extract airports table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark             -- reference to Spark session.
    * PATHS             -- paths for input and output data.
    * airport_codes_i94_df_spark -- cleaned i94 airport code Spark dataframe
    * start_str         -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * airports_table_df -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating airports_table...")
    # Create table + query
    airport_codes_i94_df_spark.createOrReplaceTempView("airports_table_DF")
    airports_table = spark.sql("""
        SELECT DISTINCT  i94_port          AS airport_id,
                         i94_airport_name  AS airport_name,
                         i94_airport_state AS airport_state
        FROM airports_table_DF             AS airports
        ORDER BY airport_name
    """)

    print("SCHEMA:")
    airports_table.printSchema()
    #print("DATA EXAMPLES:")
    #airports_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    airports_table_path = PATHS["output_data"] \
                                + "airports_table.parquet" \
                                + "_" + start_time
    print(f"OUTPUT: {airports_table_path}")
    airports_table.write.mode("overwrite")\
                        .parquet(airports_table_path)
    print("Writing airports_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    airports_table_df = spark.read\
                             .parquet(airports_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating airports_table DONE in: {total_local}\n")

    return airports_table_df

# --------------------------------------------------------
def process_time_data(spark, PATHS, i94_df_spark_clean, start_time):
    """Load input data (i94_clean),
        process the data to extract time table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark              -- reference to Spark session.
    * PATHS              -- paths for input and output data.
    * i94_df_spark_clean -- cleaned i94 Spark dataframe
    * start_str          -- Datetime when the pipeline was started.
                            Used to name parquet files.

    Output:
    * time_table_df      -- directory with parquet files
                            stored in output data path.
    """
    start_local = datetime.now()
    print("Creating time_table...")

    # Add new arrival_ts column
    print("Creating new arrival_ts column...")

    @udf(t.TimestampType())
    def get_timestamp (arrdate):
        arrdate_int = int(arrdate)
        return (datetime(1960,1,1) + timedelta(days=arrdate_int))

    i94_df_spark_clean = i94_df_spark_clean\
                        .withColumn("arrival_time", \
                                    get_timestamp(i94_df_spark_clean.arrdate))
    print("New column creation DONE.")
    # --------------------------------------------------------
    print("Creating time_table query...")
    # Create table + query
    # Extracting detailed data from arrival_ts
    i94_df_spark_clean.createOrReplaceTempView("time_table_DF")
    time_table = spark.sql("""
        SELECT DISTINCT  arrival_time             AS arrival_ts,
                         hour(arrival_time)       AS hour,
                         day(arrival_time)        AS day,
                         weekofyear(arrival_time) AS week,
                         month(arrival_time)      AS month,
                         year(arrival_time)       AS year,
                         dayofweek(arrival_time)  AS weekday
        FROM time_table_DF

    """)

    print("SCHEMA:")
    time_table.printSchema()
    #print("DATA EXAMPLES:")
    #time_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    time_table_path = PATHS["output_data"] \
                            + "time_table.parquet" \
                            + "_" + start_time
    print(f"OUTPUT: {time_table_path}")
    time_table.write.mode("append").partitionBy("year", "month")\
                    .parquet(time_table_path)
    print("Writing time_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    time_table_df = spark.read.parquet(time_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating time_table DONE in: {total_local}\n")

    return time_table_df, i94_df_spark_clean

# --------------------------------------------------------
def process_immigrations_data(spark, \
                              PATHS, \
                              i94_df_spark_clean, \
                              country_codes_i94_df_spark, \
                              airport_codes_i94_df_spark, \
                              time_table_df, \
                              start_time):
    """Load input data (i94_clean, country_code_clean,
        airport_codes_clean),
        process the data to extract immigrations table and
        store the prepered data to parquet files.

    Keyword arguments:
    * spark                 -- reference to Spark session.
    * PATHS                 -- paths for input and output data.
    * i94_df_spark_clean    -- cleaned i94 Spark dataframe
    * country_codes_i94_df_spark -- cleaned i94 country codes dataframe
    * airport_codes_i94_df_spark -- cleaned i94 airport codes dataframe
    * start_str             -- Datetime when the pipeline was started.
                                Used to name parquet files.

    Output:
    * immigrations_table_df -- directory with parquet files
                                stored in output data path.
    """
    start_local = datetime.now()
    print("Creating immigrations_table query...")
    # Join dataframes
    i94_df_spark_joined = i94_df_spark_clean\
        .join(country_codes_i94_df_spark, \
            (i94_df_spark_clean.i94cit == \
                    country_codes_i94_df_spark.i94_cit))\
        .join(airport_codes_i94_df_spark, \
            (i94_df_spark_clean.i94port == \
                    airport_codes_i94_df_spark.i94_port))\
        .join(time_table_df, \
                    i94_df_spark_clean.arrival_time == \
                    time_table_df.arrival_ts)
    # --------------------------------------------------------
    # Add new arrival_ts column
    print("Creating new immigration_id column...")
    i94_df_spark_joined = i94_df_spark_joined\
                            .withColumn("immigration_id", \
                                        monotonically_increasing_id())
    print("New column DONE.")
    # --------------------------------------------------------
    # Create table + query
    @udf(t.TimestampType())
    def get_timestamp2 (depdate):
        if depdate == "null":
            depdate_int = 0
        else:
            depdate_int = int(depdate)
        return (datetime(1960,1,1) + timedelta(days=depdate_int))

    i94_df_spark_joined = i94_df_spark_joined\
                        .withColumn("departure_date", \
                            get_timestamp2(i94_df_spark_joined.depdate))
    print("New column creation DONE.")
    print("i94_joined_SCHEMA:")
    i94_df_spark_joined.printSchema()
    # --------------------------------------------------------
    print("Creating immigrations_table query...")
    # Create table + query
    i94_df_spark_joined.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table = spark.sql("""
        SELECT DISTINCT  immigration_id AS immigration_id,
                         arrival_time   AS arrival_time,
                         year           AS arrival_year,
                         month          AS arrival_month,
                         i94_port       AS airport_id,
                         i94_cit        AS country_code,
                         admnum         AS admission_nbr,
                         i94mode        AS arrival_mode,
                         departure_date AS departure_date,
                         airline        AS airline,
                         fltno          AS flight_nbr

        FROM immigrations_table_DF immigrants
        ORDER BY arrival_time
    """)

    print("SCHEMA:")
    immigrations_table.printSchema()
    #print("DATA EXAMPLES:")
    #time_table.show(2, truncate=False)
    # --------------------------------------------------------
    print("Writing parquet files ...")
    # Write DF to parquet file:
    immigrations_table_path = PATHS["output_data"] \
                                    + "immigrations_table.parquet" \
                                    + "_" + start_time
    print(f"OUTPUT: {immigrations_table_path}")
    immigrations_table.write.mode("append")\
                            .partitionBy("arrival_year", "arrival_month")\
                            .parquet(immigrations_table_path)
    print("Writing immigrations_table parquet files DONE.")
    # --------------------------------------------------------
    # Read parquet file back to Spark:
    immigrations_table_df = spark.read.parquet(immigrations_table_path)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Creating immigrations_table DONE in: {total_local}\n")

    return immigrations_table_df

# --------------------------------------------------------
def check_data_quality( spark, \
                        round_ts, \
                        admissions_table_df, \
                        countries_table_df,
                        airports_table_df, \
                        time_table_df, \
                        immigrations_table_df, \
                        start_time):
    """Check data quality of all dimension and fact tables.

    Keyword arguments:
    * admissions_table   -- directory with admissions_table parquet files
                          stored in output_data path.
    * countries_table    -- directory with countries_table parquet files
                          stored in output_data path.
    * airports_table     -- directory with airports_table parquet files
                          stored in output_data path.
    * time_table         -- directory with time_table parquet files
                          stored in output_data path.
    * immigrations_table -- directory with immigrations_table parquet files
                          stored in output_data path.

    Output:
    *
    """
    start_local = datetime.now()
    print("Start processing data quality checks...")
    results = { "round_ts": round_ts,
                "admissions_count": 0,
                "admissions": "",
                "countries_count": 0,
                "countries": "",
                "airports_count": 0,
                "airports": "",
                "time_count": 0,
                "time": "",
                "immigrations_count": 0,
                "immigrations": ""}
    # --------------------------------------------------------
    # CHECK admissions table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking admissions table...")
    admissions_table_df.createOrReplaceTempView("admissions_table_DF")
    admissions_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM admissions_table_DF
        WHERE   admission_nbr IS NULL OR admission_nbr == "" OR
                country_code IS NULL OR country_code == ""
    """)

    # Check that table has > 0 rows
    admissions_table_df.createOrReplaceTempView("admissions_table_DF")
    admissions_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM admissions_table_DF
    """)
    if admissions_table_check1.collect()[0][0] > 0 \
        & admissions_table_check2.collect()[0][0] < 1:
        results['admissions_count'] = admissions_table_check2.collect()[0][0]
        results['admissions'] = "NOK"
    else:
        results['admissions_count'] = admissions_table_check2.collect()[0][0]
        results['admissions'] = "OK"

    print("NULLS:")
    admissions_table_check1.show(1)
    print("ROWS:")
    admissions_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK countries table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking countries table...")
    countries_table_df.createOrReplaceTempView("countries_table_DF")
    countries_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM countries_table_DF
        WHERE   country_code IS NULL OR country_code == "" OR
                country_name IS NULL OR country_name == "" OR
                iso_ccode IS NULL OR iso_ccode == "" OR
                iso_alpha_2 IS NULL OR iso_alpha_2 == "" OR
                iso_alpha_3 IS NULL OR iso_alpha_3 == "" OR
                iso_3166_2_code IS NULL OR iso_3166_2_code == "" OR
                iso_country_name IS NULL OR iso_country_name == ""
    """)

    # Check that table has > 0 rows
    countries_table_df.createOrReplaceTempView("countries_table_DF")
    countries_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM countries_table_DF
    """)

    if countries_table_check1.collect()[0][0] > 0 \
        & countries_table_check2.collect()[0][0] < 1:
        results['countries_count'] = countries_table_check2.collect()[0][0]
        results['countries'] = "NOK"
    else:
        results['countries_count'] = countries_table_check2.collect()[0][0]
        results['countries'] = "OK"

    print("NULLS:")
    countries_table_check1.show(1)
    print("ROWS:")
    countries_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK airports table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking airports table...")
    airports_table_df.createOrReplaceTempView("airports_table_DF")
    airports_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM airports_table_DF
        WHERE   airport_id IS NULL OR airport_id == "" OR
                airport_name IS NULL OR airport_name == ""
    """)

    # Check that table has > 0 rows
    airports_table_df.createOrReplaceTempView("airports_table_DF")
    airports_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM airports_table_DF
    """)

    if airports_table_check1.collect()[0][0] > 0 \
        & airports_table_check2.collect()[0][0] < 1:
        results['airports_count'] = airports_table_check2.collect()[0][0]
        results['airports'] = "NOK"
    else:
        results['airports_count'] = airports_table_check2.collect()[0][0]
        results['airports'] = "OK"

    print("NULLS:")
    airports_table_check1.show(1)
    print("ROWS:")
    airports_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK time table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking time table...")
    time_table_df.createOrReplaceTempView("time_table_DF")
    time_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM time_table_DF
        WHERE   arrival_ts IS NULL OR arrival_ts == ""
    """)


    # Check that table has > 0 rows
    time_table_df.createOrReplaceTempView("time_table_DF")
    time_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM time_table_DF
    """)
    if time_table_check1.collect()[0][0] > 0 \
        & time_table_check2.collect()[0][0] < 1:
        results['time_count'] = time_table_check2.collect()[0][0]
        results['time'] = "NOK"
    else:
        results['time_count'] = time_table_check2.collect()[0][0]
        results['time'] = "OK"

    print("NULLS:")
    time_table_check1.show(1)
    print("ROWS:")
    time_table_check2.show(1)
    # --------------------------------------------------------
    # CHECK immigrations table:
    # Check that key fields have valid values (no nulls or empty)
    print("Checking immigrations table...")
    immigrations_table_df.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table_check1 = spark.sql("""
        SELECT  COUNT(*)
        FROM immigrations_table_DF
        WHERE   immigration_id IS NULL OR immigration_id == "" OR
                arrival_time IS NULL OR arrival_time == "" OR
                arrival_year IS NULL OR arrival_year == "" OR
                arrival_month IS NULL OR arrival_month == "" OR
                airport_id IS NULL OR airport_id == "" OR
                country_code IS NULL OR country_code == "" OR
                admission_nbr IS NULL OR admission_nbr == ""
    """)

    # Check that table has > 0 rows
    immigrations_table_df.createOrReplaceTempView("immigrations_table_DF")
    immigrations_table_check2 = spark.sql("""
        SELECT  COUNT(*)
        FROM immigrations_table_DF
    """)

    if immigrations_table_check1.collect()[0][0] > 0 \
        & immigrations_table_check2.collect()[0][0] < 1:
        results['immigrations_count'] = immigrations_table_check2.collect()[0][0]
        results['immigrations'] = "NOK"
    else:
        results['immigrations_count'] = immigrations_table_check2.collect()[0][0]
        results['immigrations'] = "OK"

    print("NULLS:")
    immigrations_table_check1.show(1)
    print("ROWS:")
    immigrations_table_check2.show(1)
    # --------------------------------------------------------
    stop_local = datetime.now()
    total_local = stop_local - start_local
    print(f"Checking data quality DONE in: {total_local}\n")

    return results



def main():
    
    # Prepare configs for the pipeline.
    config_all = configparser.ConfigParser()
    config_all.read('dl.cfg')
    
    # Load the keys for 
    os.environ['AWS_ACCESS_KEY_ID']=config_all['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config_all['AWS']['AWS_SECRET_ACCESS_KEY']

    # Load JAVA, spark and hadoop configuration path directories
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    
    
    # Load the global variables
    PATHS = process_setup_variables(config_all)
    
    
    # Create Spark session
    spark = create_spark_session()
    # --------------------------------------------------------
    # Parse input data dir
    input_files = parse_input_files(PATHS, \
                                    PATHS["i94_data"], \
                                    "*.sas7bdat", \
                                    start_str)
    input_files_reordered = reorder_paths(input_files)
    PATHS["i94_files"] = input_files_reordered
    print(f"i94_files: {PATHS['i94_files']}")
    # --------------------------------------------------------
    # Process all input
    round = 0
    for filepath in PATHS["i94_files"]:
        round_ts = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
        # Process input data to staging tables.
        i94_df_spark = process_i94_data(spark, PATHS, filepath, start_str)
        airport_codes_i94_df_spark = process_i94_airport_data(  spark, \
                                                                PATHS, \
                                                                start_str)
        country_codes_i94_df_spark = process_i94_country_code_data(spark, \
                                                                PATHS, \
                                                                start_str)
        country_codes_iso_df_spark = process_iso_country_code_data(spark, \
                                                                PATHS, \
                                                                start_str)
        # --------------------------------------------------------
        # Cleaning the data:
        i94_df_spark_clean = clean_i94_data(spark, \
                                            PATHS, \
                                            i94_df_spark, \
                                            start_str)

        # Process Dimension tables.
        admissions_table_df = process_admissions_data(\
                                                spark, \
                                                PATHS, \
                                                i94_df_spark_clean, \
                                                start_str)

        countries_table_df = process_countries_data(\
                                                spark, \
                                                PATHS, \
                                                country_codes_i94_df_spark, \
                                                country_codes_iso_df_spark, \
                                                start_str)

        airports_table_df = process_airport_data(\
                                                spark, \
                                                PATHS, \
                                                airport_codes_i94_df_spark, \
                                                start_str)

        time_table_df, i94_df_spark_clean = process_time_data( \
                                                spark, \
                                                PATHS, \
                                                i94_df_spark_clean, \
                                                start_str)

        # Process Fact table.
        immigrations_table_df = process_immigrations_data( \
                                        spark,
                                        PATHS, \
                                        i94_df_spark_clean, \
                                        country_codes_i94_df_spark, \
                                        airport_codes_i94_df_spark, \
                                        time_table_df, \
                                        start_str)

        print("Checking data quality of created tables.")
        results = check_data_quality( spark, \
                                      round_ts, \
                                      admissions_table_df, \
                                      countries_table_df,
                                      airports_table_df, \
                                      time_table_df, \
                                      immigrations_table_df, \
                                      start_str)
        results_all.append(results)
        print("Data quality checks DONE.")
    print("Finished the ETL pipeline processing.")
    print("RESULTS: ")
    print(results_all)
    # --------------------------------------------------------

    print("ALL work in ETL pipeline is now DONE.")
    stop = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}"\
            .format(stop))
    print("TOTAL TIME: {}".format(stop-start))


if __name__ == "__main__":
    main()
