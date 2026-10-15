import subprocess
import time
import re
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

# ==========================================
# CONFIGURATION
# ==========================================

ROUTER_IP = "192.168.31.138"
COMMUNITY = "public"

INTERFACE = "Fa1/1"
IF_INDEX = 3

INTERVAL = 1

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "snmp"

# SNMP 32-bit Counters
IN_OID = f"1.3.6.1.2.1.2.2.1.10.{IF_INDEX}"
OUT_OID = f"1.3.6.1.2.1.2.2.1.16.{IF_INDEX}"


# ==========================================
# SNMP FUNCTION
# ==========================================

def snmp_get(oid):

    command = [
        "snmpwalk",
        "-v2c",
        "-c", COMMUNITY,
        "-On",
        "-t", "1",
        "-r", "0",
        ROUTER_IP,
        oid
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=5
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    match = re.search(
        r"Counter(?:32|64):\s*(\d+)",
        result.stdout
    )

    if not match:
        raise RuntimeError(
            f"Invalid SNMP response: {result.stdout}"
        )

    return int(match.group(1))


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)


# ==========================================
# MAIN COLLECTOR
# ==========================================

def main():

    print("=" * 60)
    print("        SNMP TRAFFIC COLLECTOR → KAFKA")
    print("=" * 60)

    print("Router       :", ROUTER_IP)
    print("Interface    :", INTERFACE)
    print("Index        :", IF_INDEX)
    print("Kafka Broker :", KAFKA_SERVER)
    print("Kafka Topic  :", KAFKA_TOPIC)

    print("=" * 60)

    old_in = snmp_get(IN_OID)
    old_out = snmp_get(OUT_OID)
    old_time = time.monotonic()

    print("\nCollecting and sending traffic data...\n")

    while True:

        time.sleep(INTERVAL)

        # Read SNMP Counters
        new_in = snmp_get(IN_OID)
        new_out = snmp_get(OUT_OID)

        new_time = time.monotonic()

        elapsed = new_time - old_time

        # Counter differences
        delta_in = (new_in - old_in) % (2 ** 32)
        delta_out = (new_out - old_out) % (2 ** 32)

        # Convert to Mbps
        in_mbps = delta_in * 8 / elapsed / 1_000_000
        out_mbps = delta_out * 8 / elapsed / 1_000_000

        # JSON Message
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "router_ip": ROUTER_IP,
            "interface": INTERFACE,
            "if_index": IF_INDEX,
            "in_octets": new_in,
            "out_octets": new_out,
            "in_mbps": round(in_mbps, 3),
            "out_mbps": round(out_mbps, 3)
        }

        # Send to Kafka
        future = producer.send(
            KAFKA_TOPIC,
            key=ROUTER_IP,
            value=message
        )

        # Wait for Kafka acknowledgement
        future.get(timeout=10)

        # Display Result
        print(
            f"IN: {in_mbps:8.3f} Mbps | "
            f"OUT: {out_mbps:8.3f} Mbps "
            f"→ Kafka [{KAFKA_TOPIC}]"
        )

        # Update counters
        old_in = new_in
        old_out = new_out
        old_time = new_time


# ==========================================
# START
# ==========================================

if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        print("\nStopping collector...")

    except Exception as error:
        print("Error:", error)

    finally:
        producer.flush()
        producer.close()
