from kafka import KafkaConsumer
import json 

consumer = KafkaConsumer(
    'ride_status',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='notification_service'
)

for msg in consumer:
    ride_status = msg.value 
    print(f"[Notification Service] Ride Status Update: User {ride_status['user_id']} - Driver {ride_status['driver']} - Status: {ride_status['status']}")
    print(f"Pickup: {ride_status['location']}")