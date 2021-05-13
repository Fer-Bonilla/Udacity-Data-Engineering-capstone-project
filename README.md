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

The project uses data from [Million Song Dataset] (https://labrosa.ee.columbia.edu/millionsong/) that is a freely-available collection of audio features and metadata for a million contemporary popular music tracks (300 GB). This data is open for exploration and research and for this project will only use a sample from the songs database and artist information in json format.
  
- **Song dataset**:  
  Json files are under */data/song_data* directory. The file format is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log dataset**: 
  Json File are under */data/log_data*. The file format is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

The data is available in the Udacity buckets 

```
  Song data: s3://udacity-dend/song_data
  Log data: s3://udacity-dend/log_data

```
The data paths are defined into the Airflow environment connection variables.


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
