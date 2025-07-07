from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='inventory_group'
)

for message in consumer:
    order = message.value 
    print(f"[Inventory Service] Received Order: {order['order_id']} - {order['product']} x{order['quantity']}")