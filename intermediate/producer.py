from kafka import KafkaProducer
import json 
import random 
import time 

users = ['Alice', 'Bob', 'Charlie', 'Diana']
locations = ['New York', 'Los Angeles', 'Chicago', 'Houston']

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

while True:
    ride_request = {
        "user_id":random.choice(users),
        "Location": random.choice(locations),
        "ride_id": random.randint(1000,9999),
        "timestamp": time.time()
    }

    print("[PRODUCER] Sent Ride Reequest: ",ride_request)
    producer.send('ride_requests',value=ride_request)

    time.sleep(2)

