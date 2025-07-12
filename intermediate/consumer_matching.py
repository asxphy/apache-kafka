from kafka import KafkaConsumer,KafkaProducer 
import json 
import random 

drivers = ['John', 'Mike', 'Sarah', 'Emma']

consumer = KafkaConsumer(
    'ride_requests',
    group_id = 'matching_service',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

for msg in consumer:
    ride_request = msg.value 
    print(ride_request)
    assigned_driver = random.choice(drivers)
    ride_status = {
        "user_id": ride_request['user_id'],
        "driver": assigned_driver,
        "status":"Driver assigned",
        "location": ride_request['Location'],
    
    }
    print("[MATCHING] Assigned Driver:",ride_status)
    producer.send('ride_status', value=ride_status)