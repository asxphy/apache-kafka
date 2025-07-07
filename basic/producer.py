from kafka import KafkaProducer
import json 
import time 
import random 

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)


products = ['laptop','phone','tablet','monitor']

while True:
    order = {
        "order_id": random.randint(1000,99999),
        "product":random.choice(products),
        "quantity": random.randint(1,10)
    }
    print(f"Producign order: {order}")
    producer.send('orders',value=order)
    time.sleep(2)