import pandas as pd
import time 

from kafka import KafkaProducer
import json
t0 = time.time()
# Function to serialize data as JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Initialize Kafka Producer
server = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

# Read the green CSV file and select required columns
df_green = pd.read_csv('green.csv.gz', usecols=['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount'])

# Iterate over records and send them to Kafka topic
for row in df_green.itertuples(index=False):
    # Convert row to dictionary
    row_dict = {col: getattr(row, col) for col in row._fields}
    # Send data to Kafka topic
    producer.send('green-trips', value=row_dict)

# Flush the producer to ensure all pending messages are sent
producer.flush()
t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
