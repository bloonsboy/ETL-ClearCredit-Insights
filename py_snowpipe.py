import os, sys, logging
import json
import uuid
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import pathlib

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
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-insert'},
    )

def save_to_snowflake(snow, batch, temp_dir, ingest_manager):
    logging.debug('inserting batch to db')
    pandas_df = pd.DataFrame(batch, columns=[
        "TXID", "TIMESTAMP", "CREDIT_PRODUCT", "LOAN_AMOUNT", "INTEREST_RATE",
        "TENOR_MONTHS", "PAYMENT_STATUS", "DAYS_PAST_DUE", "LAST_PAYMENT_DATE",
        "MONTHLY_TRANSACTIONS", "AVG_TRANSACTION_AMOUNT", "COMMON_CATEGORY",
        "CUSTOMER"
    ])
    table_name = "CONSUMER_CREDIT_RECORDS_PY_SNOWPIPE"
    stage_name = f"@%{table_name}"
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{str(uuid.uuid1())}.parquet"
    out_path = pathlib.Path(temp_dir.name) / file_name
    pq.write_table(arrow_table, out_path, use_dictionary=False,compression='SNAPPY')
    file_uri = out_path.as_uri()
    snow.cursor().execute(f"PUT '{file_uri}' {stage_name}")
    os.unlink(out_path)

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
    ingest_manager = SimpleIngestManager(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        host=host,
        user=os.getenv("SNOWFLAKE_USER"),
        pipe='INGEST.INGEST.CONSUMER_CREDIT_RECORDS_PIPE',
        private_key=private_key
    )
    for message in sys.stdin:
        if message != '\n':
            record = json.loads(message)
            batch.append((
                        record.get("txid"),
                        record.get("timestamp"),
                        record.get("credit_product"),
                        record.get("loan_amount"),
                        record.get("interest_rate"),
                        record.get("tenor_months"),
                        record.get("payment_status"),
                        record.get("days_past_due"),
                        record.get("last_payment_date"),
                        record.get("monthly_transactions"),
                        record.get("avg_transaction_amount"),
                        record.get("common_category"),
                        json.dumps(record.get("customer"))
                    ))
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