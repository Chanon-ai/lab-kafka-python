import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / 'traffic.db'

def connect():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA busy_timeout=10000')
    return con

def init_db():
    with connect() as con:
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('''CREATE TABLE IF NOT EXISTS traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            router_ip TEXT NOT NULL,
            interface TEXT NOT NULL,
            if_index INTEGER,
            in_octets INTEGER,
            out_octets INTEGER,
            in_mbps REAL NOT NULL,
            out_mbps REAL NOT NULL
        )''')
        con.execute('CREATE INDEX IF NOT EXISTS idx_traffic_time ON traffic(timestamp)')
        con.execute('CREATE INDEX IF NOT EXISTS idx_traffic_device_time ON traffic(router_ip, interface, timestamp)')
