import json
import paramiko
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC = "router-commands"

ROUTERS = {
    "192.168.31.132": "R1",
    "192.168.31.133": "R2"
}

ROUTER_USERNAME = "admin"
ROUTER_PASSWORD = "cisco"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id="router-workers",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Worker started")
print(f"Waiting for messages from topic: {TOPIC}")


def execute_command(router_ip, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        print(f"\nConnecting to router {router_ip}...")

        ssh.connect(
            hostname=router_ip,
            username=ROUTER_USERNAME,
            password=ROUTER_PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=10
        )

        print("SSH connected")
        print(f"Executing command: {command}")

        stdin, stdout, stderr = ssh.exec_command(command)

        output = stdout.read().decode()
        error = stderr.read().decode()

        print("\n========== ROUTER OUTPUT ==========")

        if output:
            print(output)

        if error:
            print("ERROR:")
            print(error)

        print("===================================")

    except Exception as e:
        print(f"SSH Error: {e}")

    finally:
        ssh.close()


for message in consumer:
    event = message.value

    print("\nReceived Kafka message:")
    print(json.dumps(event, indent=2))

    router_ip = event.get("router_ip")
    command = event.get("command")

    router_name = ROUTERS.get(router_ip, "Unknown Router")

    print("\n========== NEW JOB ==========")
    print(f"Router  : {router_name}")
    print(f"IP      : {router_ip}")
    print(f"Command : {command}")
    print("=============================")

    if router_ip and command:
        execute_command(router_ip, command)
    else:
        print("Invalid Kafka message")
