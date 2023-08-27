import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('en_IN')

for _ in range(25):
    first_name = fake.first_name()
    last_name = fake.last_name()
    address = 'Delhi'
    pincode = '122009'
    phone_number = '91' + ''.join([str(random.randint(0, 9)) for _ in range(8)])
    joining_date = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2023, 8, 20)).strftime('%Y-%m-%d')

    print(f"INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('{first_name}', '{last_name}', '{address}', '{pincode}', '{phone_number}', '{joining_date}');")
