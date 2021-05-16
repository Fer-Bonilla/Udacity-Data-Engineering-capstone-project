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

          ```  
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
          ```  

Ethereum (ETH/USDT) 1m Dataset

          ```
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
          ```
  
## Database Model

The database will be designed for analytics using Fact and Dimensions tables on a Star Schema architecture, and staging tables to read data from s3 data storage:

**Staging Tables**

    *Table: staging_btc

    *Columns:
        - Timestamp
        - Open
        - High
        - Low
        - Close
        - Volume_btc
        - Volume_currency
        - Weighted_price


    *Table: staging_eth

    *Columns:
        - timestamp
        - open
        - high
        - close
        - volume
        - close_time
        - quote_av
        - trades
        - tb_base_av
        - tb_quote_Av
        - ignore





**Transformed series**

    *Table: btc_timeseries

    *Columns:
        - timestamp
        - year
        - month
        - day
        - hour
        - btc_open
        - btc_high
        - btc_low
        - btc_close
        - btc_volume


    *Table: eth_timeseries

    *Columns:
        - timestamp
        - year (Partition Key)
        - month
        - day
        - hour
        - eth_open
        - eth_high
        - eth_low
        - eth_close
        - eth_volume


**combined series**

    *Table: crypto_timeseries

    *Columns:
        - year (Partition Key)
        - month
        - day
        - hour
        - btc_open
        - btc_high
        - btc_low
        - btc_close
        - btc_volume
        - eth_open
        - eth_high
        - eth_low
        - eth_close
        - eth_volume




### Logic model

![Logic model](https://github.com/Fer-Bonilla/Udacity-Data-Engineering-capstone-project/blob/main/conceptual_model-spark.png)



## Project structure

The project structure is based on the Udacity's project template:

```
+ data    + btc_data     : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
          + eth_data     : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

+ capstone_project.ipynb : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
+ etl.py                 : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
+ dl.cfg                 : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

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
