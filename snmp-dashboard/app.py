from flask import Flask, jsonify, render_template, request
from datetime import datetime, timedelta, timezone
from database import connect, init_db

app = Flask(__name__)
init_db()

@app.get('/')
def index():
    return render_template('index.html')

@app.get('/api/devices')
def devices():
    with connect() as db:
        rows = db.execute('SELECT DISTINCT router_ip, interface FROM traffic ORDER BY router_ip, interface').fetchall()
    return jsonify([dict(r) for r in rows])

@app.get('/api/traffic')
def traffic():
    router = request.args.get('router', '192.168.31.134')[:100]
    iface = request.args.get('interface', 'Fa1/1')[:100]
    period = request.args.get('period', '1h')
    minutes = {'15m':15, '1h':60, '6h':360, '24h':1440, '7d':10080}.get(period, 60)
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat(timespec='milliseconds')
    # Up to 1200 chronologically ordered points; sample by relative row number.
    with connect() as db:
        total = db.execute('SELECT COUNT(*) FROM traffic WHERE router_ip=? AND interface=? AND timestamp>=?',
                           (router,iface,since)).fetchone()[0]
        stride = max(1, (total+1199)//1200)
        rows = db.execute('''SELECT timestamp,in_mbps,out_mbps,in_octets,out_octets FROM (
            SELECT timestamp,in_mbps,out_mbps,in_octets,out_octets,
                   ROW_NUMBER() OVER (ORDER BY timestamp,id) AS rn
            FROM traffic WHERE router_ip=? AND interface=? AND timestamp>=?
        ) WHERE (rn-1) % ? = 0 ORDER BY timestamp''', (router,iface,since,stride)).fetchall()
        latest = db.execute('''SELECT timestamp,in_mbps,out_mbps,in_octets,out_octets FROM traffic
            WHERE router_ip=? AND interface=? ORDER BY timestamp DESC,id DESC LIMIT 1''', (router,iface)).fetchone()
    return jsonify({'points':[dict(r) for r in rows], 'latest':dict(latest) if latest else None,
                    'count':total, 'router':router, 'interface':iface})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8085, debug=False)
