# capstone_project
## Overview
This ETL pipeline processes real estate sales data using Medallion Architecture. 

##Architecture

CSV File -> Bronze Layer -> Silver Layer -> Gold Layer
          (raw_data.py)    (clean.py)      (aggregate.py)

## Pipeline Stages
Bronze Layer
  * Loading the csv into MongoDB w/o transformation
Silver Layer
  * Cleaning the data
      * Standardizing column names, trimming whitespace, converting dates to ISO format,
        removing duplicates, and handling missing values.
      * Enforcing non-negative amounts
   
Gold Layer
  * Yearly: transaction count by year
  * Town: transaction count by town
  * Property Type: transaction count by type
