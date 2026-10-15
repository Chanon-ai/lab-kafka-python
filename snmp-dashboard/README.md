# SNMP Traffic Dashboard

A clean, responsive dashboard for the existing `collector.py` → Kafka `snmp` topic pipeline. This project includes a **new history-saving worker**; leave your existing collector unchanged.

## Setup (Ubuntu VM)

```bash
cd ~/lab/lab-kafka-python
source venv/bin/activate
python -m pip install flask kafka-python
```

Extract the `snmp-dashboard` folder under `~/lab/lab-kafka-python/`.

Start Kafka with your existing Compose configuration:

```bash
docker compose -f kafka-local/docker-compose.yml up -d
```

In three terminals (with `venv` active):

1. `python collector.py` (your existing Kafka producer)
2. `python snmp-dashboard/worker.py` (new history worker; writes `snmp-dashboard/traffic.db`)
3. `python snmp-dashboard/app.py` (web server)

Open `http://localhost:8085` on the Ubuntu VM, or `http://<ubuntu-ip>:8085` from Windows if VM networking and firewall permit.

The history worker uses a **different consumer group** (`snmp-dashboard-storage`) so it does not compete with your original `worker/worker.py` consumer. You can keep both running. `auto_offset_reset='earliest'` replays Kafka history only on first use of the group; how much history is available depends on Kafka retention. Run one history worker instance only, to avoid duplicate inserts. The database is SQLite and persists when restarted.

Defaults: Kafka `localhost:9092`, topic `snmp`, router `192.168.31.134`, interface `Fa1/1`. To override Kafka address or topic, set environment variables `KAFKA_SERVER` and `KAFKA_TOPIC` before starting the history worker.

Chart.js is served via a CDN, so the browser needs internet to display the graph. The API and data storage work offline if the rest of the environment is available.

**Note**: This dashboard does not create network traffic. Low/zero Mbps values reflect the SNMP measurements. If your IPTerm topology doesn't pass traffic through Fa1/1, the chart remains near zero.

## Dark theme update
This version includes a dark, responsive interface. It uses the same SQLite database and Kafka worker as the original. Stop your web server, back up the original folder, replace only `static/style.css`, `templates/index.html`, and (if needed) `app.py` for port 8085, then start Flask again. Do not overwrite `traffic.db`.
