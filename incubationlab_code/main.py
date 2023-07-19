import random
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from faker import Faker
from datetime import datetime

fake = Faker()

actives_records = []
for _ in range(1000):
    date = fake.date_between(start_date='-30d', end_date='today')
    month = date.strftime("%B")
    record = {
        'advertising_id': fake.uuid4(),
        'city': fake.city(),
        'location_category': fake.word(),
        'location_granularities': fake.word(),
        'location_source': ','.join(fake.words(random.randint(1, 3))),
        'state': fake.state(),
        'timestamp': random.randint(0, int(datetime.now().timestamp())),
        'user_id': fake.uuid4(),
        'user_latitude': round(random.uniform(-90, 90), 7),
        'user_longitude': round(random.uniform(-180, 180), 7),
        'month': month,
        'date': date,
    }
    actives_records.append(record)

viewrship_records = []
for _ in range(1000):
    date = fake.date_between(start_date='-30d', end_date='today')
    month = date.strftime("%B")
    record = {
        'advertising_id': fake.uuid4(),
        'channel_genre': fake.word(),
        'channel_name': fake.word(),
        'city': fake.city(),
        'device': fake.word(),
        'device_type': fake.word(),
        'duration': random.randint(1, 3600),
        'grid_id': fake.uuid4(),
        'language': fake.language_code(),
        'location_category': fake.word(),
        'location_granularities': fake.word(),
        'location_source': ','.join(fake.words(random.randint(1, 3))),
        'record_timestamp': random.randint(0, int(datetime.now().timestamp())),
        'show_genre': fake.word(),
        'show_name': fake.word(),
        'state': fake.state(),
        'user_lat': round(random.uniform(-90, 90), 7),
        'user_long': round(random.uniform(-180, 180), 7),
        'month': month,
        'date': date,
    }
    viewrship_records.append(record)

actives_table = pa.Table.from_pandas(pd.DataFrame(actives_records))
viewrship_table = pa.Table.from_pandas(pd.DataFrame(viewrship_records))

actives_file_path = 'actives_records.parquet'
viewrship_file_path = 'viewrship_records.parquet'

pq.write_table(actives_table, actives_file_path)
pq.write_table(viewrship_table, viewrship_file_path)

print("Actives Records:")
for record in actives_records:
    print(record)

print("\nViewrship Records:")
for record in viewrship_records:
    print(record)
