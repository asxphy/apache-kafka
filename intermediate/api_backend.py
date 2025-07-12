# api_backend.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import time

app = FastAPI()

# ✅ Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Request schema
class RideRequest(BaseModel):
    user_id: str
    Location: str
    ride_id: str

@app.post("/rides")
def create_ride(ride: RideRequest):
    # Build ride data with timestamp
    ride_data = ride.dict()
    ride_data["timestamp"] = time.time()

    # Send to Kafka topic
    producer.send("ride_requests", value=ride_data)
    producer.flush()  # Ensure it goes immediately

    print(f"[API] Produced Ride Request: {ride_data}")

    return {"status": "Ride request sent", "data": ride_data}
