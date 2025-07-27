from kafka import KafkaProducer
import psutil
import time 
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092'
                         , value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'cpu_metrics'

while True:
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_times = psutil.cpu_times()._asdict()
    payload = {
        'cpu_percent': cpu_percent,
        'cpu_times': cpu_times,
        'timestamp': time.time()
    }
    producer.send(topic, value=payload)
    print(f"Sent: {payload}")
    time.sleep(1)
