import os, sys, logging
import json
import snowflake.connector


from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization


load_dotenv()
logging.basicConfig(level=logging.WARN)
snowflake.connector.paramstyle = 'qmark'




def connect_snow():
   # Connexion avec clé privée (optionnel, tu peux aussi utiliser mot de passe si défini dans .env)
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




def save_to_snowflake(snow, message):
   record = json.loads(message)
   logging.debug('inserting record to db')


   row = (
       record['txid'],
       record['timestamp'],
       record['credit_product'],
       record['loan_amount'],
       record['interest_rate'],
       record['tenor_months'],
       record['payment_status'],
       record['days_past_due'],
       record.get('last_payment_date'),
       record['monthly_transactions'],
       record['avg_transaction_amount'],
       record['common_category'],
       json.dumps(record['customer'])  # JSON string → sera parsé en VARIANT
   )


   sql = """
       INSERT INTO CONSUMER_CREDIT_RECORDS
       ("TXID","TIMESTAMP","CREDIT_PRODUCT","LOAN_AMOUNT","INTEREST_RATE","TENOR_MONTHS",
        "PAYMENT_STATUS","DAYS_PAST_DUE","LAST_PAYMENT_DATE","MONTHLY_TRANSACTIONS",
        "AVG_TRANSACTION_AMOUNT","COMMON_CATEGORY","CUSTOMER")
       SELECT ?,?,?,?,?,?,?,?,?,?,?,?,PARSE_JSON(?)
   """


   snow.cursor().execute(sql, row)
   logging.debug(f"inserted record {record}")




if __name__ == "__main__":
   snow = connect_snow()
   for message in sys.stdin:
       if message != '\n':
           save_to_snowflake(snow, message)
       else:
           break
   snow.close()
   logging.info("Ingest complete")