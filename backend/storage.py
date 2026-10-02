"""SQLite persistence. Daily rollups keep long-range queries bounded."""
import json, os, sqlite3, threading
from functools import wraps
from datetime import datetime, timezone

def _serialized(method):
    """Serialize access to the shared SQLite connection across API threads."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)
    return wrapped

class Store:
    def __init__(self, path=None):
        self.path = path or os.getenv("DATABASE_PATH", "fleetpulse.db")
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS vehicles(id TEXT PRIMARY KEY,type TEXT,fuel_type TEXT,expected_l_per_100km REAL,utilisation_pct REAL DEFAULT 0,status TEXT DEFAULT 'inactive',latitude REAL,longitude REAL,speed_kmh REAL DEFAULT 0,fuel_level_pct REAL DEFAULT 100,fuel_rate_lph REAL DEFAULT 0,odometer_km REAL DEFAULT 0,idle_seconds INTEGER DEFAULT 0,last_update TEXT);
        CREATE TABLE IF NOT EXISTS seen_events(event_id TEXT PRIMARY KEY,seen_at TEXT);
        CREATE TABLE IF NOT EXISTS telemetry(id INTEGER PRIMARY KEY,event_id TEXT UNIQUE,vehicle_id TEXT,timestamp TEXT,speed_kmh REAL,fuel_level_pct REAL,fuel_rate_lph REAL,engine_on INTEGER,idle INTEGER,odometer_km REAL);
        CREATE INDEX IF NOT EXISTS telemetry_vehicle_ts ON telemetry(vehicle_id,timestamp DESC);
        CREATE TABLE IF NOT EXISTS daily(vehicle_id TEXT,day TEXT,distance_km REAL DEFAULT 0,fuel_l REAL DEFAULT 0,idle_seconds INTEGER DEFAULT 0,active_seconds INTEGER DEFAULT 0,events INTEGER DEFAULT 0,PRIMARY KEY(vehicle_id,day));
        CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,vehicle_id TEXT,type TEXT,severity TEXT,timestamp TEXT,description TEXT,estimated_cost_inr REAL,UNIQUE(vehicle_id,type,timestamp));
        """)
        self.db.commit()

    def seed(self, count):
        types = ["Van", "Truck", "Pickup", "Coach"]
        with self.lock, self.db:
            n = self.db.execute("SELECT count(*) FROM vehicles").fetchone()[0]
            if n >= count: return
            self.db.executemany("INSERT OR IGNORE INTO vehicles(id,type,fuel_type,expected_l_per_100km,utilisation_pct,status,latitude,longitude,fuel_level_pct,odometer_km) VALUES(?,?,?,?,?,?,?,?,?,?)", ((f"VH-{i:06d}", types[i%4], "Diesel" if i%5 else "Petrol", [8.2,24.0,11.5,28.0][i%4], float(20+(i*17)%76), "active" if i%10<7 else "inactive", 12.8+(i%900)/1000, 77.6+(i%1200)/1000, float(25+(i*31)%76), float((i*7919)%320000)) for i in range(1,count+1)))

    @_serialized
    def vehicle(self, vid):
        row = self.db.execute("SELECT * FROM vehicles WHERE id=?", (vid,)).fetchone()
        return dict(row) if row else None

    @_serialized
    def ingest(self, event):
        ts = event["timestamp"]
        day = ts[:10]
        with self.lock, self.db:
            if not self.db.execute("INSERT OR IGNORE INTO seen_events VALUES(?,?)",(event["event_id"],datetime.now(timezone.utc).isoformat())).rowcount: return False, []
            vehicle = self.vehicle(event["vehicle_id"])
            if not vehicle: raise ValueError("Unknown vehicle_id")
            previous = self.db.execute("SELECT * FROM telemetry WHERE vehicle_id=? ORDER BY timestamp DESC LIMIT 1",(event["vehicle_id"],)).fetchone()
            stale = bool(vehicle["last_update"] and ts < vehicle["last_update"])
            idle_seconds = max((vehicle["idle_seconds"] or 0) + 10 if event["idle"] else 0, int(event.get("idle_duration_minutes", 0) * 60))
            dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
            fuel = event["fuel_rate_lph"] / 360
            distance = max(0, event["odometer_km"]-(previous["odometer_km"] if previous else event["odometer_km"]))
            vehicle["recent_l_per_100km"] = (fuel*100/distance) if distance > .02 else 0
            vehicle["idle_seconds"] = idle_seconds
            alerts = [] if stale else __import__("backend.processing", fromlist=["detect_alerts"]).detect_alerts(event,vehicle,idle_seconds)
            self.db.execute("INSERT INTO telemetry(event_id,vehicle_id,timestamp,speed_kmh,fuel_level_pct,fuel_rate_lph,engine_on,idle,odometer_km) VALUES(?,?,?,?,?,?,?,?,?)",(event["event_id"],event["vehicle_id"],ts,event["speed_kmh"],event["fuel_level_pct"],event["fuel_rate_lph"],event["engine_on"],event["idle"],event["odometer_km"]))
            if not stale:
                self.db.execute("UPDATE vehicles SET status=?,latitude=?,longitude=?,speed_kmh=?,fuel_level_pct=?,fuel_rate_lph=?,odometer_km=?,idle_seconds=?,last_update=? WHERE id=?",("idle" if event["idle"] else "active" if event["engine_on"] else "inactive",event["latitude"],event["longitude"],event["speed_kmh"],event["fuel_level_pct"],event["fuel_rate_lph"],event["odometer_km"],idle_seconds,ts,event["vehicle_id"]))
            self.db.execute("INSERT INTO daily(vehicle_id,day,distance_km,fuel_l,idle_seconds,active_seconds,events) VALUES(?,?,?,?,?,?,1) ON CONFLICT(vehicle_id,day) DO UPDATE SET distance_km=distance_km+excluded.distance_km,fuel_l=fuel_l+excluded.fuel_l,idle_seconds=idle_seconds+excluded.idle_seconds,active_seconds=active_seconds+excluded.active_seconds,events=events+1",(event["vehicle_id"],day,distance,fuel,idle_seconds and 10 or 0,10 if event["engine_on"] else 0))
            for a in alerts: self.db.execute("INSERT OR IGNORE INTO alerts(vehicle_id,type,severity,timestamp,description,estimated_cost_inr) VALUES(?,?,?,?,?,?)",(event["vehicle_id"],a["type"],a["severity"],ts,a["description"],a["estimated_cost_inr"]))
            self.db.execute("DELETE FROM telemetry WHERE id NOT IN (SELECT id FROM telemetry ORDER BY id DESC LIMIT 100000)")
            self.db.execute("DELETE FROM seen_events WHERE rowid NOT IN (SELECT rowid FROM seen_events ORDER BY rowid DESC LIMIT 500000)")
            return True, alerts

    @_serialized
    def summary(self):
        row=self.db.execute("SELECT count(*) total,sum(status IN ('active','idle')) connected,avg(utilisation_pct) utilisation,sum(idle_seconds) idle FROM vehicles").fetchone()
        fuel=self.db.execute("SELECT coalesce(sum(fuel_l),0) liters FROM daily WHERE day>=date('now','-6 days')").fetchone()[0]
        alerts=self.db.execute("SELECT count(*) FROM alerts WHERE timestamp>=datetime('now','-1 day')").fetchone()[0]
        return {"total_vehicles":row["total"],"connected":row["connected"] or 0,"connected_pct":round((row["connected"] or 0)*100/max(row["total"],1),1),"utilisation_pct":round(row["utilisation"] or 0,1),"fuel_cost_inr":round(fuel*100,2),"fuel_liters":round(fuel,1),"active_alerts":alerts,"idle_seconds":row["idle"] or 0}

    @_serialized
    def vehicles(self,page=1,size=25,search="",status=""):
        where=[]; args=[]
        if search: where.append("(id LIKE ? OR type LIKE ?)"); args += [f"%{search}%"]*2
        if status: where.append("status=?"); args.append(status)
        clause=" WHERE "+" AND ".join(where) if where else ""
        total=self.db.execute("SELECT count(*) FROM vehicles"+clause,args).fetchone()[0]
        rows=self.db.execute("SELECT * FROM vehicles"+clause+" ORDER BY last_update DESC,id LIMIT ? OFFSET ?",args+[size,(page-1)*size]).fetchall()
        return {"items":[dict(r) for r in rows],"page":page,"page_size":size,"total":total,"pages":(total+size-1)//size}

    @_serialized
    def alerts(self,page=1,size=25,severity=""):
        w=" WHERE severity=?" if severity else ""; a=[severity] if severity else []
        total=self.db.execute("SELECT count(*) FROM alerts"+w,a).fetchone()[0]
        rows=self.db.execute("SELECT alerts.*,vehicles.type FROM alerts LEFT JOIN vehicles ON vehicles.id=alerts.vehicle_id"+w+" ORDER BY id DESC LIMIT ? OFFSET ?",a+[size,(page-1)*size]).fetchall()
        return {"items":[dict(r) for r in rows],"page":page,"page_size":size,"total":total,"pages":(total+size-1)//size}

    @_serialized
    def fuel(self,days=7):
        rows=self.db.execute("SELECT day,sum(fuel_l) liters,sum(distance_km) distance FROM daily WHERE day>=date('now',?) GROUP BY day ORDER BY day",(f"-{days-1} days",)).fetchall()
        top=self.db.execute("SELECT vehicles.id,vehicles.type,sum(daily.fuel_l) liters,sum(daily.distance_km) distance FROM daily JOIN vehicles ON vehicles.id=daily.vehicle_id WHERE day>=date('now',?) GROUP BY vehicles.id ORDER BY liters DESC LIMIT 8",(f"-{days-1} days",)).fetchall()
        return {"trend":[dict(r) for r in rows],"top_vehicles":[dict(r) for r in top],"liters":round(sum(r['liters'] for r in rows),1),"cost_inr":round(sum(r['liters'] for r in rows)*100,2),"waste_liters":round(sum(r['liters'] for r in rows)*.08,1),"anomaly_count":self.db.execute("SELECT count(*) FROM alerts WHERE type='fuel_anomaly' AND timestamp>=datetime('now',?)",(f"-{days} days",)).fetchone()[0]}

    @_serialized
    def utilisation(self,days=7):
        counts=self.db.execute("SELECT status,count(*) n FROM vehicles GROUP BY status").fetchall()
        under=self.db.execute("SELECT id,type,utilisation_pct,status FROM vehicles WHERE utilisation_pct<35 ORDER BY utilisation_pct LIMIT 8").fetchall()
        trend=self.db.execute("SELECT day,avg(CASE WHEN active_seconds>0 THEN min(100,active_seconds*100.0/86400) ELSE 0 END) pct FROM daily WHERE day>=date('now',?) GROUP BY day ORDER BY day",(f"-{days-1} days",)).fetchall()
        return {"counts":{r['status']:r['n'] for r in counts},"trend":[dict(r) for r in trend],"under_utilised":[dict(r) for r in under],"average_pct":self.summary()['utilisation_pct']}
