# ELT Process Orchestration – Assignment
**Author**: Dominik Nykiel, s24813 

## Running the Code
To test the code, simply use the command:
```
docker compose up -d
``` 
to spin up all Docker containers, then log in to the Airflow UI at `localhost:8080`.
To get the password, use the command:
```
docker compose logs airflow | grep password
```
Remember to create a data folder in the project's root directory and place the appropriate data file there.
Then launch both `producer.py` and `consumer.py` from the kafka folder to begin the streaming ingestion. 
Alternatively, `nyc_taxi_batch.py` can be used to ingest data only through Airflow and Python. 

## Problem Description
This assignment is an extension of a data pipeline project based on the medallion architecture. The datasets used contain records of taxi trips within New York City, sourced from the official NYC Taxi & Limousine Commission website. The goal of the assignment is to build an initial data processing pipeline using process orchestration, in such a way that the entire process requires as little physical user interaction as possible. 

## Data Description
Link to data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Each record in the dataset describes a single taxi trip registered by the carrier operating the vehicle. Each record contains the following fields: 

- VendorID – Numeric identifier of the carrier.
- tpep_pickup_datetime – Date and time the trip started.
- tpep_dropoff_datetime – Date and time the trip ended.
- passenger_count – Number of passengers on the trip.
- trip_distance – Length of the trip (in miles).
- RatecodeID – Numeric identifier of the fare calculation method.
- store_and_fwd – Flag indicating whether trip data was sent to the carrier immediately or stored in the vehicle's memory first.
- PULocationID – Numeric identifier of the trip's pickup location.
- DOLocationID – Numeric identifier of the trip's drop-off location.
- payment_type – Numeric identifier of the payment method.
- fare_amount – Base fare for the trip (in dollars and cents).
- extra – Any additional charges (in dollars and cents).
- mta_tax – Tax amount calculated based on the applicable fare standard.
- tip_amount – Tip amount. Only tips paid by card are counted.
- tolls_amount – Total toll charges during the trip.
- total_amount – Sum of all charges and tips.
- congestion_surcharge – Additional surcharge for trips during peak hours. 

The data is divided by year, and each year is further divided by month, meaning there are twelve datasets per year. 

## Technologies Used
### Apache Airflow
![Airflow logo](/images/airflow_logo.png) 

Apache Airflow is a tool for visually creating, organizing, and monitoring workflows and running task chains. In this project, it serves as the main orchestration engine for the pipeline.
### DBT
![DBT logo](/images/dbt_logo.png) 

DBT is a tool for transforming data loaded into a database, using SQL files. In this project, it is used to build the silver and gold layers of the medallion architecture. 
### Docker
![Docker logo](/images/docker_logo.png) 

Docker is a platform and software for application containerization, enabling programs to run in virtual containers. In this project, Docker is used to containerize Airflow and the PostgreSQL database, in order to get the pipeline up and running more quickly. 

## Data Flow Diagram
![Data pipeline diagram extended](/images/pipeline_diagram_en.png)
## How the Data Was Processed
The data was downloaded from the NYC TLC website in parquet format. The data files were placed in the data folder and then processed according to the medallion architecture.

- In the bronze layer, using Kafka's producer-consumer structure, raw data is loaded into the database in a table with general column formats. A field describing the data source (file name) and the timestamp of when the data was loaded are added. The **pandas** library is used here to load the data into a data frame.
- In the silver layer, data is copied from the bronze layer with conversion to appropriate column types. An initial filtering of records is performed, and records containing information that is invalid according to the accepted data rules are flagged. **DBT** is used for data processing in this layer, in combination with the appropriate SQL files.
- In the gold layer, tables are created containing specific information based on aggregations of data from the silver table. Tables were created to analyze daily trip statistics, taxi operator statistics, and tables containing suspicious records based on various criteria. **DBT** tools are also applied here to produce the tables.

## What Changed Compared to the Previous Assignment?

- When running PostgreSQL via Docker, a volume is used, allowing data to persist after containers are shut down.
- Data and tables are no longer deleted on each script run; the bronze and silver layers are built incrementally.
- DBT tests have been added to verify the quality of input data – currently only at the silver layer.

## What Could Be Improved?

- Improving the orchestration part of the Airflow project by using a full Docker implementation instead of the trimmed-down `standalone` version.
- Better use of DBT, adding new data processing features. The current setup is a fairly simple scheme.
- Adding a pgAdmin container or connecting the database to another graphical interface to make it easier to browse the database.
- Adding data fetching or streaming from the NYC API instead of using physical files.

Overall, the project represents an acceptable introduction to Airflow and DBT, but could be improved through better use of the functionality that these technologies provide.
# ERD for Individual Layers
## Bronze Layer
![ERD diagram for bronze layer of taxi trip database](/images//bronze_ERD.png)

## Silver Layer
![ERD diagram for silver layer of taxi trip database](/images/silver_ERD_new.png)

## Gold Layer
![ERD diagram for gold layer of taxi trip database](/images/gold_ERD.png)