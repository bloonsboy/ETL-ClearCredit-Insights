# 🚀 Snowpipe Tutorial: File Upload & Copy for Kpop Demon Hunter Merchandise

This tutorial demonstrates how to use **Snowpipe** for efficient batch processing of customer orders. Snowpipe uses serverless infrastructure to ingest data from files uploaded to Snowflake stages.

## 🎯 What You'll Learn

- **Snowpipe concepts**: Serverless data ingestion
- **Batch processing**: Efficient handling of large datasets
- **File formats**: Parquet files with compression
- **Cost optimization**: Pay only for compute time used
- **Performance tuning**: Batch size optimization

## 📋 Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Python environment with required packages
- RSA key pair authentication configured

## 🏗️ Step 1: Create Table and Snowpipe

Run this SQL in your Snowflake worksheet:

```sql
-- Create the table for Kpop Demon Hunter merchandise orders
USE ROLE INGEST;
-- CREATE OR REPLACE TABLE CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE (
--     TXID VARCHAR(255),
--     RFID VARCHAR(255), 
--     ITEM VARCHAR(255),                    -- Merchandise products
--     PURCHASE_TIME TIMESTAMP,
--     EXPIRATION_TIME DATE,
--     DAYS NUMBER,
--     NAME VARCHAR(255),
--     ADDRESS VARIANT,                    -- JSON: {street_address, city, state, postalcode}
--     PHONE VARCHAR(255),
--     EMAIL VARCHAR(255),
--     EMERGENCY_CONTACT VARIANT           -- JSON: {name, phone}
-- );

-- FOR CREDIT
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

-- Create the Snowpipe to handle the ingest
-- CREATE OR REPLACE PIPE CONSUMER_CREDIT_RECORDS_PIPE AS 
-- COPY INTO CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE
-- FILE_FORMAT=(TYPE='PARQUET') 
-- MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;

CREATE OR REPLACE PIPE CONSUMER_CREDIT_RECORDS_PIPE AS 
COPY INTO CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE
FILE_FORMAT=(TYPE='PARQUET') 
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;
```

## 🐍 Step 2: Install Required Packages

Add these packages to your `environment.yml`:

```yaml
dependencies:
  - pandas=1.5.3
  - pyarrow=10.0.1
  - snowflake-ingest=1.0.10
```

Or install directly:
```bash
pip install pandas pyarrow snowflake-ingest
```

## 🐍 Step 3: Create Python Snowpipe Script

Create `py_snowpipe.py` with the following code:

```python
import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)


def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )


def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=["TXID","RFID","ITEM","PURCHASE_TIME", "EXPIRATION_TIME","DAYS","NAME","ADDRESS","PHONE","EMAIL", "EMERGENCY_CONTACT"])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path =  f"{temp_dir.name}/{file_name}"
    pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
    snow.cursor().execute("PUT 'file://{0}' @%CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE".format(out_path))
    os.unlink(out_path)
    # send the new file to snowpipe to ingest (serverless)
    resp = ingest_manager.ingest_files([StagedFile(file_name, None),])
    logging.info(f"response from snowflake for file {file_name}: {resp['responseCode']}")


if __name__ == "__main__":    
    args = sys.argv[1:]
    batch_size = int(args[0])
    snow = connect_snow()
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n"
    host = os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com"
    ingest_manager = SimpleIngestManager(account=os.getenv("SNOWFLAKE_ACCOUNT"),
                                         host=host,
                                         user=os.getenv("SNOWFLAKE_USER"),
                                         pipe='INGEST.INGEST.CONSUMER_CREDIT_RECORDS_PIPE',
                                         private_key=private_key)
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((record['txid'],record['rfid'],record["item"],record["purchase_time"],record["expiration_time"],record['days'],record['name'],record['address'],record['phone'],record['email'], record['emergency_contact']))
            if len(batch) == batch_size:
                save_to_snowflake(snow, batch, temp_dir, ingest_manager)
                batch = []
        else:
            break    
    if len(batch) > 0:
        save_to_snowflake(snow, batch, temp_dir, ingest_manager)
    temp_dir.cleanup()
    snow.close()
    logging.info("Ingest complete")
```

## 🚀 Step 4: Test Snowpipe Processing

### Small Test (1 record)
```bash
python data_generator.py 1 | python py_snowpipe.py 1
```

### Medium Test (100 records)
```bash
python data_generator.py 100 | python py_snowpipe.py 10
```

### Large Test (10,000 records)
```bash
python data_generator.py 10000 | python py_snowpipe.py 1000
```

### Full Dataset Test
```bash
# Process your full dataset with 10K batch size
gunzip -c data.json.gz | python py_snowpipe.py 10000
```

## 📊 Step 5: Monitor Snowpipe Performance

### Check Data Ingestion
```sql
-- Check total records (may take 1-2 minutes to appear)
SELECT COUNT(*) FROM CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE;

-- Check recent data
SELECT * FROM CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE 
ORDER BY TIMESTAMP DESC 
LIMIT 10;
```

### Monitor Snowpipe Usage
```sql
-- Check Snowpipe processing history
-- SELECT 
--     PIPE_NAME,
--     FILE_NAME,
--     FILE_SIZE,
--     STAGE_LOCATION,
--     STATUS,
--     LOAD_TIME,
--     FIRST_ERROR_MESSAGE
-- FROM INFORMATION_SCHEMA.PIPE_USAGE_HISTORY 
-- WHERE PIPE_NAME = 'CONSUMER_CREDIT_RECORDS_PIPE'
-- ORDER BY LOAD_TIME DESC;
SELECT 
    FILE_NAME,
    TABLE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE
FROM 
  TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE',
    START_TIME=>DATEADD(hours, -24, CURRENT_TIMESTAMP())
  ))
ORDER BY LAST_LOAD_TIME DESC;
```

### Performance Analysis    
```sql
-- Analyze processing efficiency
-- SELECT 
--     DATE_TRUNC('hour', LOAD_TIME) as hour,
--     COUNT(*) as files_processed,
--     SUM(FILE_SIZE) as total_bytes,
--     AVG(FILE_SIZE) as avg_file_size
-- FROM INFORMATION_SCHEMA.PIPE_USAGE_HISTORY 
-- WHERE PIPE_NAME = 'CONSUMER_CREDIT_RECORDS_PIPE'
-- GROUP BY hour
-- ORDER BY hour DESC;

SELECT 
    DATE_TRUNC('hour', START_TIME) as hour,
    SUM(FILES_INSERTED) as files_processed,
    SUM(BYTES_INSERTED) / (1024*1024) as total_megabytes_inserted
FROM 
    TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
        DATE_RANGE_START=>DATEADD('day', -7, CURRENT_TIMESTAMP()),
        PIPE_NAME=>'INGEST.INGEST.CONSUMER_CREDIT_RECORDS_PIPE'
    ))
GROUP BY hour
ORDER BY hour DESC;
```

## 🎯 Step 6: Understanding Snowpipe Benefits

### Cost Efficiency
- **Serverless**: Pay only for compute time used
- **Batch processing**: More efficient than row-by-row inserts
- **File size optimization**: Larger files = better cost efficiency

### Performance Benefits
- **High throughput**: Can process millions of records
- **Asynchronous**: Non-blocking processing
- **Scalable**: Automatically handles load spikes

### When to Use Snowpipe
✅ **Good for:**
- Batch processing (100+ records)
- Large files (approaching 100MB)
- Cost-sensitive applications
- High-volume data ingestion

❌ **Avoid for:**
- Real-time streaming (use Kafka + Snowpipe)
- Very small batches (< 100 records)
- Interactive applications

## 🔧 Step 7: Performance Optimization

### Batch Size Guidelines

| Records | Batch Size | File Size | Efficiency |
|---------|------------|-----------|------------|
| 1-100   | 1-10       | < 1MB     | ⚠️ Low     |
| 100-1K  | 50-100     | 1-10MB    | ✅ Good    |
| 1K-10K  | 500-2K     | 10-50MB   | ✅ Excellent |
| 10K+    | 5K-10K     | 50-100MB  | 🎯 Optimal |

### Test Different Batch Sizes
```bash
# Test small batches (less efficient)
python data_generator.py 1000 | python py_snowpipe.py 10

# Test medium batches (good efficiency)
python data_generator.py 1000 | python py_snowpipe.py 100

# Test large batches (optimal efficiency)
python data_generator.py 1000 | python py_snowpipe.py 1000
```

## 📈 Step 8: Business Analytics

### Product Performance Analysis
```sql
-- Top selling merchandise
SELECT 
    ITEM,
    COUNT(*) as total_orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as market_share
FROM CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE 
GROUP BY ITEM 
ORDER BY total_orders DESC;
```

### Customer Insights
```sql
-- Geographic distribution
SELECT 
    ADDRESS:state as state,
    COUNT(*) as orders,
    COUNT(DISTINCT NAME) as unique_customers
FROM CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE 
WHERE ADDRESS IS NOT NULL
GROUP BY state 
ORDER BY orders DESC;
```

### Time-based Analysis
```sql
-- Orders by hour of day
SELECT 
    HOUR(PURCHASE_TIME) as hour_of_day,
    COUNT(*) as orders
FROM CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE 
GROUP BY hour_of_day 
ORDER BY hour_of_day;
```

## 🚨 Troubleshooting

### Common Issues

**"No data appearing in table"**
- Snowpipe is asynchronous - wait 1-2 minutes
- Check PIPE_USAGE_HISTORY for errors
- Verify file upload to stage

**"Permission denied"**
- Ensure user has INGEST role
- Check stage permissions
- Verify pipe ownership

**"File format errors"**
- Ensure Parquet files are properly formatted
- Check column names match table schema
- Verify JSON fields are properly serialized

### Debug Queries
```sql
-- Check pipe status
SHOW PIPES;

-- Check stage files
LIST @%CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE;

-- Check for errors
-- SELECT * FROM INFORMATION_SCHEMA.PIPE_USAGE_HISTORY 
-- WHERE STATUS = 'LOAD_FAILED';

SELECT
    FILE_NAME,
    TABLE_NAME,
    STATUS,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    LAST_LOAD_TIME
FROM
  TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME=>'CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE',
    START_TIME=>DATEADD(hours, -24, CURRENT_TIMESTAMP())
  ))
WHERE STATUS = 'LOAD_FAILED'
ORDER BY LAST_LOAD_TIME DESC;

```

## 🎯 Key Takeaways

1. **Snowpipe is ideal for batch processing** - not real-time streaming
2. **Batch size matters** - larger batches = better efficiency
3. **File format is important** - Parquet with compression is optimal
4. **Cost is based on compute time** - not data volume
5. **Processing is asynchronous** - expect 1-2 minute delays

## 🚀 Next Steps

- **Scale up**: Process 100K+ records
- **Optimize**: Find your optimal batch size
- **Monitor**: Set up alerts for processing failures
- **Automate**: Schedule regular data loads
- **Integrate**: Connect to other data sources

## 📚 Additional Resources

- [Snowpipe Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
- [Parquet File Format](https://parquet.apache.org/)
- [Snowflake Cost Optimization](https://docs.snowflake.com/en/user-guide/cost-understanding-warehouse-costs)

---

**🎉 Congratulations!** You've learned how to use Snowpipe for efficient batch data processing. This pattern is used by major companies for high-volume data ingestion! 🚀
