/* GROUPE 7
NAME 

Use Case: Credit Risk & Customer Behavior Simulation

We used our data generator to simulate end-to-end credit lifecycle records for a financial institution. Each JSON record represents a customer’s credit profile, their loan details, repayment behavior, and transaction patterns.
Why generate this data?
The goal is to create a synthetic but realistic dataset that can be ingested into Snowflake for ETL/ELT pipelines, allowing us to demonstrate:
Credit risk analysis (on-time vs late/defaulted payments, days past due).
Portfolio analytics (distribution of loan types, interest rates, loan sizes, tenors).
Customer segmentation (by demographics, income, employment status, transaction behavior).
Behavioral insights (transaction frequency, average spend, common merchant categories).


Key elements in the dataset
Credit product details → type of loan, amount, interest rate, tenor.
Repayment behavior → payment status (on time, late, default), days past due, last payment date.
Aggregated spending patterns → number of transactions, average ticket size, most common spending category.
Customer demographics → name, DOB, address, phone, email, employment status, annual income.
Metadata → transaction ID and timestamp for traceability.


Business applications
ETL Pipeline 1: Land raw credit records → clean/transform → create an analytical table for loan portfolio monitoring (average interest rates, approval mix, delinquency).
ETL Pipeline 2: Land the same records into another flow → aggregate customer behavior and repayment KPIs → build dashboards for risk scoring and collections strategy.
*/

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

--- Generator
```python

import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime

load_dotenv()
fake = Faker()

credit_products = [
    "Personal Loan", "Credit Card", "Auto Loan", "Mortgage", 
    "Student Loan", "Business Loan", "Buy Now Pay Later", 
    "Overdraft Facility", "Line of Credit", "Microfinance Loan"
]


transaction_categories = [
    "Groceries", "Restaurants", "Online Shopping", "Utilities", 
    "Healthcare", "Entertainment", "Travel", "Insurance", 
    "Education", "Other"
]

def print_credit_record():
    global credit_products, transaction_categories, fake
    state = fake.state_abbr()
    
    credit_record = {
        'txid': str(uuid.uuid4()),   # identifiant unique
        'timestamp': datetime.utcnow().isoformat(),
        
        # Produit de crédit et montant
        'credit_product': fake.random_element(elements=credit_products),
        'loan_amount': round(random.uniform(500, 100000), 2),
        'interest_rate': round(random.uniform(2.5, 18.0), 2),
        'tenor_months': fake.random_int(min=6, max=360),
        
        # Comportement de remboursement
        'payment_status': fake.random_element(elements=["On Time", "Late", "Default"]),
        'days_past_due': fake.random_int(min=0, max=180),
        'last_payment_date': fake.date_between(start_date="-2y", end_date="today").isoformat(),
        
        # Transactions agrégées
        'monthly_transactions': random.randint(5, 100),
        'avg_transaction_amount': round(random.uniform(10, 500), 2),
        'common_category': fake.random_element(elements=transaction_categories),
        
        # Données démographiques client
        'customer': {
            'name': fake.name(),
            'dob': fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
            'address': {
                'street_address': fake.street_address(),
                'city': fake.city(),
                'state': state,
                'postalcode': fake.postalcode_in_state(state)
            },
            'phone': fake.phone_number(),
            'email': fake.email(),
            'employment_status': fake.random_element(elements=["Employed", "Unemployed", "Self-Employed", "Retired"]),
            'annual_income': round(random.uniform(15000, 200000), 2),
        }
    }
    d = json.dumps(credit_record) + '\n'
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_credit_record()
    print('')

```
--- Insert
```python
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
