from kafka import KafkaConsumer
import json 

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='billing_group'
)

for message in consumer:
    order = message.value
    print(f"[Billing Service] Billed Order: {order['order_id']} - ${order['quantity'] * 10}")