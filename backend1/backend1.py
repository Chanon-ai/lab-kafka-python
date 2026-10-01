from fastapi import FastAPI
from kafka import KafkaProducer
import json
import uuid

app = FastAPI(
    title="Cisco Router Command API",
    description="Send Cisco CLI commands to Kafka for Worker execution",
    version="1.0.0"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "router-commands"
ROUTERS = {
    "R1": "192.168.31.132",
    "R2": "192.168.31.133"
}

@app.get("/")
def root():
    return {
        "message": "Backend1 is running",
        "docs": "/docs"
    }


@app.post("/command")
def send_command(router: str, command: str):

    router_ip = ROUTERS.get(router)

    if not router_ip:
        return {"error": "Invalid router"}

    event = {
        "request_id": str(uuid.uuid4()),
        "router_ip": router_ip,
        "command": command
    }

    producer.send(TOPIC, value=event)
    producer.flush()

    return {
        "status": "sent",
        "topic": TOPIC,
        "event": event
    }
