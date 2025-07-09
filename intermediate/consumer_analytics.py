from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ride_requests',
    group_id='analytics_service',
    bootstrap_servers='localhost:9092', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',)

total_rides = 0

for msg in consumer:
    ride = msg.value
    total_rides += 1
    print(f"[Analytics] Total Rides processed: {total_rides}")