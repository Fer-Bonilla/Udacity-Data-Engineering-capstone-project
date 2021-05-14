# Udacity Data Engineering capstone project

Create a Data Pipeline to process Bitcoin and Ethereum daily prices from CSV files and add to the Data Warehouse, and then can be used to run price prediction models using Machine Learning Time series analysis. The pipeline includes data loading, cleaning, transformation, and aggregation to make the data available to train the ML models. In this project, the main goal is to build the data pipeline to load data to the Data Warehouse and make it available to run ML Models for price prediction. 

Project sections:

- Problem understanding
- Data description
- Database Model
- Project structure
- ETL Pipeline description
- Instructions to run the pipeline

## Problem understanding

This project defines the pipeline to load historical data of Bitcoin and Ethereum blockchains and create a Data Lake. The process includes data formatting, cleaning, and transformation. 

## Data description

Datasets used are obtained from Kaggle's datasets, from these repositories:

Bitcoin Historical Data

* Source: https://www.kaggle.com/mczielinski/bitcoin-historical-data
* Description: Bitcoin data at 1-min intervals from select exchanges, Jan 2012 to March 2021
* Format: Unique CSV file
* Fields: - Timestamp
          - Open
          - High
          - Low
          - Close
          - Volume_(BTC)
          - Volume_(Currency)
          - Weighted_Price
* Time period: 2012-01-01 to 2021-3-31
Ethereum (ETH/USDT) 1m Dataset

* Source: https://www.kaggle.com/priteshkeleven/ethereum-ethusdt-1m-dataset
* Description: Ethereum dataset with 1 minute interval from 17-8-2017 to 03-2-2021
* Format: CSV for each month
* Fields: - timestamp
          - open
          - high
          - low
          - close
          - volume
          - close_time
          - quote_av
          - trades
          - tb_base_av
          - tb_quote_av
          - ignore

* Time period: 17-8-2017 to 03-2-2021
  
- **Song dataset**:  
  Json files are under */data/song_data* directory. The file format is:

## Database Model

The database will be designed for analytics using Fact and Dimensions tables on a Star Schema architecture, and staging tables to read data from s3 data storage:

**Staging Tables**

```
  staging_events - Load the raw data from log events json files.
  artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

  staging_songs
  num_songs	artist_id	artist_latitude	artist_longitude	artist_location	artist_name	song_id	title	duration	year
```  

**Fact Table**
```
  songplays - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

**Dimension Tables**

```
  users - users in the app: user_id, first_name, last_name, gender, level
  songs - songs in music database: song_id, title, artist_id, year, duration
  artists - artists in music database: artist_id, name, location, latitude, longitude
  time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday
```

### Logic model

![Logic model](https://github.com/Fer-Bonilla/Udacity-Data-Engineering-data-pipelines-with-airflow/blob/main/redshift-udacity/DefaultLayout.svg)


## Project structure

The project structure is based on the Udacity's project template:

```
+ airflow + dags
          + plugings  + helpers   + sql_queries.py: Insert sql stament definitions
                      + operators + data_quality.py: This operator implements the data quality verification task, based on the BaseOperator
                                  + load_dimension.py: This operator implements the LoadDimensionOperator class that execute the data load process from staging tables to dimension tables.
                                  + load_fact.py: This operator implements the LoadFactOperator class that execute the data load process from staging tables to fact table.
                                  + stage_redshift.py: This operator implements the data quality verification task, based on the BaseOperator
          + create_tables.sql : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

```

## ETL Pipeline description

### The ETL is defined in the airflow configuration. The first step executes de tables creation, then the data load into staging tables is executed, next data load into fact table ins executed and the last part is load data into dimensions tables. At the end, the quality check is executed counting the rows number in the dimension’s tables.


### ETL pipeline diagram

![ETL pipeline diagram](https://github.com/Fer-Bonilla/Udacity-Data-Engineering-data-pipelines-with-airflow/blob/main/images/airflow_pipeline.png)

## Instructions to run the pipeline

A. Components required

 1.	AWS amazon account
 2.	User created on IAM AWS and administrative role to connect from remote connection
 3.	Redshift cluster created in the AWS services console
 4.	Jupyter notebooks environment available
 5.	Airflow environment 

B Running the pipeline

 1.	Clone the repository
 2.	Create IAM role and user
 3.	Create the Redshift cluster and get the connection data
 4.	Initialize the airflow service
 5.	Configure the connection values and access key in the airflow administration options (connections)
 6.	Execute the DAG
 7.	Verify the log


## Author 
Fernando Bonilla [linkedin](https://www.linkedin.com/in/fer-bonilla/)
