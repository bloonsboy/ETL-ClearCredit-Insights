## ClearCredit Insights - ETL/ELT Pipeline Project

**Group 7 : Elea Nizam, Noa Sebag, Matteo Nobile, Nathan Novier**

Concept: A fintech analytics platform to help financial institutions better understand consumer credit behaviour. This project demonstrates the data ingestion pipelines into a centralized Snowflake data warehouse.

### 🚀 Quick Start Guide

#### 1. Environment Setup
Create the Conda environment to install all Python packages:
```bash
conda env create -f environment.yaml
```
Activate the environment before running any script:
```bash
conda activate credit-insights-env
```
Configure credentials: Create a .env file and fill it with your Snowflake account details (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, PRIVATE_KEY).

#### 2. Snowflake Setup
Run the following SQL commands directly in a Snowflake worksheet to prepare the entire environment.

##### Part 1: Environment Setup (Run as ACCOUNTADMIN)
```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS INGEST;
CREATE ROLE IF NOT EXISTS INGEST;
GRANT USAGE ON WAREHOUSE INGEST TO ROLE INGEST;
GRANT OPERATE ON WAREHOUSE INGEST TO ROLE INGEST;

CREATE DATABASE IF NOT EXISTS INGEST;
USE DATABASE INGEST;
CREATE SCHEMA IF NOT EXISTS INGEST;
USE SCHEMA INGEST;

GRANT OWNERSHIP ON DATABASE INGEST TO ROLE INGEST;
GRANT OWNERSHIP ON SCHEMA INGEST.INGEST TO ROLE INGEST;

CREATE OR REPLACE USER INGEST 
    PASSWORD='YOUR_SECURE_PASSWORD_HERE' 
    LOGIN_NAME='INGEST'
    MUST_CHANGE_PASSWORD=FALSE
    DISABLED=FALSE
    DEFAULT_WAREHOUSE='INGEST'
    DEFAULT_NAMESPACE='INGEST.INGEST'
    DEFAULT_ROLE='INGEST';

GRANT ROLE INGEST TO USER INGEST;
SET MY_USER = CURRENT_USER();
GRANT ROLE INGEST TO USER IDENTIFIER($MY_USER);
```
##### Part 2: Table & Pipe Creation (Run as INGEST)
After running the script above, switch to the INGEST role (`USE ROLE INGEST;`) and run the following:
```sql
USE ROLE INGEST;
USE WAREHOUSE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

-- Table for simple, line-by-line INSERTs
CREATE OR REPLACE TABLE CONSUMER_CREDIT_RECORDS (
    TXID VARCHAR(255) NOT NULL,
    TIMESTAMP TIMESTAMP NOT NULL,
    CREDIT_PRODUCT VARCHAR(100) NOT NULL,
    LOAN_AMOUNT NUMBER(18,2) NOT NULL,
    INTEREST_RATE NUMBER(5,2) NOT NULL,
    TENOR_MONTHS NUMBER NOT NULL,
    PAYMENT_STATUS VARCHAR(50) NOT NULL,
    DAYS_PAST_DUE NUMBER NOT NULL,
    LAST_PAYMENT_DATE DATE,
    MONTHLY_TRANSACTIONS NUMBER NOT NULL,
    AVG_TRANSACTION_AMOUNT NUMBER(10,2) NOT NULL,
    COMMON_CATEGORY VARCHAR(100) NOT NULL,
    CUSTOMER VARIANT NOT NULL,
    PRIMARY KEY (TXID)
);

-- Table for high-performance Snowpipe ingestion
CREATE OR REPLACE TABLE CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE (
    TXID VARCHAR(255),
    TIMESTAMP TIMESTAMP,
    CREDIT_PRODUCT VARCHAR(100),
    LOAN_AMOUNT NUMBER(18,2),
    INTEREST_RATE NUMBER(5,2),
    TENOR_MONTHS NUMBER,
    PAYMENT_STATUS VARCHAR(50),
    DAYS_PAST_DUE NUMBER,
    LAST_PAYMENT_DATE DATE,
    MONTHLY_TRANSACTIONS NUMBER,
    AVG_TRANSACTION_AMOUNT NUMBER(10,2),
    COMMON_CATEGORY VARCHAR(100),
    CUSTOMER VARIANT
);

-- Create the Pipe for automation
CREATE OR REPLACE PIPE CONSUMER_CREDIT_RECORDS_PIPE AS 
COPY INTO CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE
FROM @%CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE
FILE_FORMAT=(TYPE='PARQUET') 
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;
```

### ▶️ Running the Ingestion Pipelines
Here are the main commands to generate and load data into Snowflake.

##### Method A: SQL Insert
```bash
python data_generator.py 100 | python py_insert.py
```
##### Method B: Batch
```bash
python data_generator.py 100000 | python file_insert.py 10000
```

##### Method C: Snowpipe
**The recommended method.**

```bash
python data_generator.py 100000 | python py_snowpipe.py 10000
```

(Optional) Generating a Compressed File First
###### On Linux/macOS:
```bash
python data_generator.py 100000 | gzip > data.json.gz
```
To load from this file:
```bash
gunzip -c data.json.gz | python py_snowpipe.py 10000
```

###### On Windows (PowerShell):

```bash
python data_generator.py 100000 > data.json
Compress-Archive -Path .\data.json -DestinationPath data.json.zip
```