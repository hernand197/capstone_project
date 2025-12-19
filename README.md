# capstone_project
## Overview
This ETL pipeline processes real estate sales data using Medallion Architecture. 

## Architecture

CSV File -> Bronze Layer -> Silver Layer -> Gold Layer
          (raw_data.py)    (clean.py)      (aggregate.py)

## Pipeline Stages
Bronze Layer (raw_data.py)
  * Loading the csv into MongoDB w/o transformation

Silver Layer  (clean.py)
  * Cleaning the data
      * Standardizing column names, trimming whitespace, converting dates to ISO format,
        removing duplicates, and handling missing values.
      * Enforcing non-negative amounts
   
Gold Layer (aggregate.py)
  * Yearly: transaction count by year
  * Town: transaction count by town
  * Property Type: transaction count by type

## Project Structure
~~~
captone_project/  
  data/  
      Real_Estate_Sales_2001-2023_GL.csv  
  src/
    raw_data.py         #csv to mongodb
    clean.py            #data cleaning
    aggregate.py        #aggregations
    schemas.py          #pydantic validation schema
  tests/
    test_raw_data.py          #testing
    test_clean.py
    test_schemas.py
  main.py
  docker-compose.yml
  pyproject.tomll
     

