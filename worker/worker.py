import json
from datetime import datetime

from kafka import KafkaConsumer

# ==========================================
# CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "snmp"
GROUP_ID = "snmp-worker"


# ==========================================
# KAFKA CONSUMER
# ==========================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,

    bootstrap_servers=[KAFKA_SERVER],

    group_id=GROUP_ID,

    auto_offset_reset="latest",

    enable_auto_commit=True,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


# ==========================================
# PROCESS SNMP DATA
# ==========================================

def process_traffic(data):

    timestamp = data.get("timestamp", "N/A")
    router_ip = data.get("router_ip", "Unknown")
    interface = data.get("interface", "Unknown")
    if_index = data.get("if_index", "N/A")

    in_octets = data.get("in_octets", 0)
    out_octets = data.get("out_octets", 0)

    in_mbps = data.get("in_mbps", 0)
    out_mbps = data.get("out_mbps", 0)

    print("\n" + "=" * 55)
    print("            SNMP TRAFFIC WORKER")
    print("=" * 55)

    print(f"Timestamp   : {timestamp}")
    print(f"Router IP   : {router_ip}")
    print(f"Interface   : {interface}")
    print(f"ifIndex     : {if_index}")

    print("-" * 55)

    print(f"IN Octets   : {in_octets:,}")
    print(f"OUT Octets  : {out_octets:,}")

    print("-" * 55)

    print(f"Input Rate  : {in_mbps:.3f} Mbps")
    print(f"Output Rate : {out_mbps:.3f} Mbps")

    print("=" * 55)


# ==========================================
# MAIN
# ==========================================

def main():

    print("=" * 55)
    print("             KAFKA SNMP WORKER")
    print("=" * 55)

    print("Kafka Broker :", KAFKA_SERVER)
    print("Kafka Topic  :", KAFKA_TOPIC)
    print("Consumer ID  :", GROUP_ID)

    print("=" * 55)

    print("\nWaiting for SNMP traffic messages...\n")

    for message in consumer:

        try:
            data = message.value

            process_traffic(data)

        except Exception as error:
            print("Error processing message:", error)


# ==========================================
# START WORKER
# ==========================================

if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        print("\nStopping Kafka Worker...")

    except Exception as error:
        print("Kafka Error:", error)

    finally:
        consumer.close()
        print("Kafka Consumer closed.")
