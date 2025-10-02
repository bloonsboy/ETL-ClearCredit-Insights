import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import datetime

load_dotenv()
fake = Faker()

CREDIT_PRODUCTS = [
   "Personal Loan", "Credit Card", "Auto Loan", "Mortgage",
   "Student Loan", "Business Loan", "Buy Now Pay Later",
   "Overdraft Facility", "Line of Credit", "Microfinance Loan"
]

TRANSACTION_CATEGORIES = [
   "Groceries", "Restaurants", "Online Shopping", "Utilities",
   "Healthcare", "Entertainment", "Travel", "Insurance",
   "Education", "Other"
]

def print_credit_record():
    global CREDIT_PRODUCTS, TRANSACTION_CATEGORIES, fake
    state = fake.state_abbr()

    credit_record = {
        'txid': str(uuid.uuid4()),   # unique ID
        'timestamp': datetime.utcnow().isoformat(), # current timestamp

        # Credit product and amount
        'credit_product': fake.random_element(elements=CREDIT_PRODUCTS), # type of credit product
        'loan_amount': round(random.uniform(500, 500000), 2), # loan amount between 500 and 500k
        'interest_rate': round(random.uniform(1.5, 10.0), 2), # interest rate between 1.5% and 10%
        'tenor_months': fake.random_int(min=6, max=360), # loan duration between 6 months and 30 years
      
        # Payment status
        'payment_status': fake.random_element(elements=["On Time", "Late", "Default"]), # payment status
        'days_past_due': 0 if fake.random_element(elements=["On Time", "Default"]) == "On Time" else fake.random_int(min=1, max=180), # days past due
        'last_payment_date': fake.date_between(start_date="-2y", end_date="today").isoformat(), # last payment date
      
        # Transaction history
        'monthly_transactions': random.randint(5, 100), # number of transactions in the last month
        'avg_transaction_amount': round(random.uniform(10, 500), 2), # average transaction amount
        'common_category': fake.random_element(elements=TRANSACTION_CATEGORIES), # most common transaction category
      
        # Customer details
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
