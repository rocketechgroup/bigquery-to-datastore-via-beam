from google.cloud import bigquery
from faker import Faker
import datetime

# Initialize Faker and BigQuery client
fake = Faker()
client = bigquery.Client()

# Define the dataset and table
dataset_id = 'bigquery_to_datastore_via_beam'
table_id = 'fake_pii_data'
table_ref = client.dataset(dataset_id).table(table_id)

create_table_query = f"""
CREATE TABLE `{dataset_id}.{table_id}` (
    uuid STRING,
    full_name STRING,
    email STRING,
    address STRING,
    phone_number STRING,
    birthdate DATE,
    last_updated_timestamp TIMESTAMP
);
"""
try:
    client.query(create_table_query).result()
except Exception as e:
    pass

# Generate 100 rows of random PII data
rows_to_insert = [
    {
        'uuid': fake.uuid4(),
        'full_name': fake.name(),
        'email': fake.email(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'birthdate': fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
        'last_updated_timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    }
    for _ in range(10)
]

# Upload data to BigQuery
errors = client.insert_rows_json(table_ref, rows_to_insert)

if errors:
    print("Errors occurred:", errors)
else:
    print("Data uploaded successfully.")
