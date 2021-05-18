# Udacity Data Engineering capstone project

Create a Data Pipeline to process Bitcoin and Ethereum daily prices from CSV files and add to the Data Warehouse, and then can be used to run price prediction models using Machine Learning Time series analysis. The pipeline includes data loading, cleaning, transformation, and aggregation to make the data available to train the ML models. In this project, the main goal is to build the data pipeline to load data to the Data Warehouse and make it available to run ML Models for price prediction. 

Project sections:

- Problem understanding
- Data description
- Database Model
- Project structure
- ETL Pipeline description
- Data dictionary
- Project Write up
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
+ data    + btc_data     : BTC historical prices data csv files 
          + eth_data     : ETH historical prices data csv files 

+ capstone_project.ipynb : Notebook with all the ETL process building and explanation
+ etl.py                 : ETL process developed on Pyspark
+ dl.cfg                 : Global variables configuration

```

## ETL Pipeline description

          1. Define the global variables in the configuration file (dl.cfg)
          2. Read data from CSV files from INPUT_FILE Directory into Spark dataframe
          3. Save Spark dataframes to staging parquet file
          4. Read BTC parket file and drop null values.
          5. Transform BTC timestamp in to (year, month, day, hour)
          6. Transform ETH timestamp in to (year, month, day, hour)
          7. Join BTC and ETH spark dataframes
          8. Save data to table crypto_timeseries
          9. Run the Quality control check

## Data dictionary

**Table btc_timeseries**

|    Field   | Data Type | NULL |                          Description                         |    Source   |
|:----------:|:---------:|:----:|:------------------------------------------------------------:|:-----------:|
| timestamp  | Integer   | NO   | Timestamp from   original source                             | staging_btc |
| year       | Integer   | NO   | Integer   representing the year obtained from the timestamp  | staging_btc |
| month      | Integer   | NO   | Integer   representing the month obtained from the timestamp | staging_btc |
| day        | Integer   | NO   | Integer   representing the day obtained from the timestamp   | staging_btc |
| hour       | Integer   | NO   | Integer   representing the hour obtained from the timestamp  | staging_btc |
| btc_open   | double    | NO   | BTC price at   opening                                       | staging_btc |
| btc_high   | double    | NO   | Highest BTC price   registered for the period                | staging_btc |
| btc_low    | double    | NO   | Lowest BTC price   registered for the period                 | staging_btc |
| btc_close  | double    | NO   | BTC Price at time   zone close                               | staging_btc |
| btc_volume | double    | NO   | Amount of BTC that   was interchanged                        | staging_btc |


**Table eth_timeseries**

|    Field   | Data Type | NULL |                          Description                         |    Source   |
|:----------:|:---------:|:----:|:------------------------------------------------------------:|:-----------:|
| timestamp  | Integer   | NO   | Timestamp from   original source                             | staging_eth |
| year       | Integer   | NO   | Integer   representing the year obtained from the timestamp  | staging_eth |
| month      | Integer   | NO   | Integer   representing the month obtained from the timestamp | staging_eth |
| day        | Integer   | NO   | Integer   representing the day obtained from the timestamp   | staging_eth |
| hour       | Integer   | NO   | Integer   representing the hour obtained from the timestamp  | staging_eth |
| eth_open   | double    | NO   | ETH price at   opening                                       | staging_eth |
| eth_high   | double    | NO   | Highest ETH price   registered for the period                | staging_eth |
| eth_low    | double    | NO   | Lowest ETH price   registered for the period                 | staging_eth |
| eth_close  | double    | NO   | ETH Price at time   zone close                               | staging_eth |
| eth_volume | double    | NO   | Amount of ETH that   was interchanged                        | staging_eth |


**Table crypto_timeseries**

|    Field   | Data Type | NULL |                          Description                         |     Source     |
|:----------:|:---------:|:----:|:------------------------------------------------------------:|:--------------:|
| timestamp  | Integer   | NO   | Timestamp from   original source                             | btc_timeseries |
| year       | Integer   | NO   | Integer   representing the year obtained from the timestamp  | btc_timeseries |
| month      | Integer   | NO   | Integer   representing the month obtained from the timestamp | btc_timeseries |
| day        | Integer   | NO   | Integer   representing the day obtained from the timestamp   | btc_timeseries |
| hour       | Integer   | NO   | Integer   representing the hour obtained from the timestamp  | btc_timeseries |
| btc_open   | double    | NO   | BTC price at   opening                                       | btc_timeseries |
| btc_high   | double    | NO   | Highest BTC price   registered for the period                | btc_timeseries |
| btc_low    | double    | NO   | Lowest BTC price   registered for the period                 | btc_timeseries |
| btc_close  | double    | NO   | BTC Price at time   zone close                               | btc_timeseries |
| btc_volume | double    | NO   | Amount of BTC that   was interchanged                        | btc_timeseries |
| eth_open   | double    | NO   | ETH price at   opening                                       | eth_timeseries |
| eth_high   | double    | NO   | Highest ETH price   registered for the period                | eth_timeseries |
| eth_low    | double    | NO   | Lowest ETH price   registered for the period                 | eth_timeseries |
| eth_close  | double    | NO   | ETH Price at time   zone close                               | eth_timeseries |
| eth_volume | double    | NO   | Amount of ETH that   was interchanged                        | eth_timeseries |


## Projec write up


### Rationale for the tools selection:

   - Better knowledge developing on Python and Pyspark
   - Easy access to run local environment for Pyspark and cloud using AWS Spark clusters.
   - PySpark shares some similarities with Python pandas and then is easy to develop with the framework.
   - Easy use of the dataframes style data access and SQL 
   

### How often ETL script should be run:

   - Due to the high dynamic in the cryptocurrency market new info is added each minute, for that reason could be necessary to execute the pipeline at least one time daily. But is not necessary to update all the info only the new one, then the pipeline can be modified to append information, or maybe use a data streaming framework like Kafka can be a better option
   

### Other scenarions (what to consider in them):

   - Huge increase in data (Data 100x):  This case can happen if more cryptos are included, which means that each crypto requires more space. In this case, we can use a bigger spark cluster on AWS or another cloud service. Other options include use another database for staging and simplify some columns in the datasets. In that case, we can set up a parallel process on spark to improve the performance
   
   - The pipelines would be run on a daily basis by 7 am every day: In this case, could be helpful to use a task manager as ML Flow to integrate and schedule the job execution. To update the data every morning is required to integrate the data with providers.

   - The database needed to be accessed by 100+ people: For this case, many users can access the data to create specific queries, views, or run their ML models. in that case, a very flexible database is required using the cloud services to provide the data with low latency times.
   
   
### Potential further work:    

   - Integrate with other data sources as exchanges to integrate more data and more up-to-date price information.
   - This approach can require a data streaming service that appends new data
   - Integrate with AUTO ML tools or ML flows tools to automatize the prediction models after data is updated.
   - Improve the process using MLflow or other data pipelines flow manager.
   - Improve the pipeline design and implementation


## Instructions to run the pipeline

A. Components required

 1.	PySpark and Spark services available
 2.	Jupyter notebooks environment available

B Running the pipeline

 1.	Clone the repository
 2.	Download the zip data files from the Kaggle repositories
 3.	Configure the dl.cfg global variables values
 4.	Run the Pipeline with Python etl.py
 

## Author 
Fernando Bonilla [linkedin](https://www.linkedin.com/in/fer-bonilla/)
