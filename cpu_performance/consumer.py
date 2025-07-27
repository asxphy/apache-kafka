from kafka import KafkaConsumer
import json
import sqlalchemy
from sqlalchemy import text
engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:password@localhost:3306/cpu_monitoring"
)
conn = engine.connect()

consumer = KafkaConsumer(
    'cpu_metrics',
    bootstrap_servers='localhost:9092',
    group_id='cpu_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

for msg in consumer:
    data = msg.value
    cpu_percent = data['cpu_percent']
    cpu_times = data['cpu_times']
    ts = data['timestamp']

    insert_stmt = text("""
    INSERT INTO cpu_metrics (cpu_percent, user_time, system_time, idle_time, timestamp)
    VALUES (:cpu_percent, :user_time, :system_time, :idle_time, :ts)
""")
    with engine.begin() as conn: 
        conn.execute(insert_stmt, {
            "cpu_percent": cpu_percent,
            "user_time": cpu_times['user'],
            "system_time": cpu_times['system'],
            "idle_time": cpu_times['idle'],
            "ts": ts
        })  
    print(f"Inserted: {data}")