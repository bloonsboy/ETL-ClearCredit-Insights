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
