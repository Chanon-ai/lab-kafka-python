"""Consume SNMP measurements from Kafka and persist them for dashboard history."""
import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from database import connect, init_db

BROKER = os.getenv('KAFKA_SERVER', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'snmp')
GROUP = os.getenv('KAFKA_GROUP', 'snmp-dashboard-storage')

def normalize_time(value):
    dt = datetime.fromisoformat(value.replace('Z', '+00:00')) if value else datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds')

def main():
    init_db()
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers=[BROKER], group_id=GROUP,
        auto_offset_reset='earliest', enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode('utf-8'))
    )
    print(f'Listening to {TOPIC} on {BROKER}; saving to traffic.db', flush=True)
    try:
        for record in consumer:
            try:
                msg = record.value
                row = (
                    normalize_time(msg.get('timestamp')),
                    str(msg['router_ip']), str(msg['interface']),
                    int(msg.get('if_index', 0)),
                    int(msg.get('in_octets', 0)), int(msg.get('out_octets', 0)),
                    float(msg['in_mbps']), float(msg['out_mbps'])
                )
                with connect() as db:
                    db.execute('''INSERT INTO traffic
                        (timestamp,router_ip,interface,if_index,in_octets,out_octets,in_mbps,out_mbps)
                        VALUES (?,?,?,?,?,?,?,?)''', row)
                consumer.commit()
                print(f'{row[0]} {row[1]} {row[2]} IN={row[6]:.3f} OUT={row[7]:.3f} Mbps', flush=True)
            except (KeyError, TypeError, ValueError) as err:
                print(f'Skipping malformed message: {err}', flush=True)
                consumer.commit()
    except KeyboardInterrupt:
        print('Stopping worker')
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
