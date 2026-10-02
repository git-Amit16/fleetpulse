"""Small dependency-free JSON API and SSE server for the local demo."""
import json, os, queue, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from backend.processing import validate_event, ValidationError
from backend.storage import Store

store=Store(); store.seed(int(os.getenv("VEHICLE_COUNT","100000")))
subscribers=set(); sub_lock=threading.Lock()

def publish(kind,data):
    payload=json.dumps({"type":kind,"data":data},separators=(",",":"))
    with sub_lock:
        for q in list(subscribers):
            try: q.put_nowait(payload)
            except queue.Full:
                try: q.get_nowait(); q.put_nowait(payload)
                except queue.Empty: pass

class Handler(BaseHTTPRequestHandler):
    server_version="FleetPulse/1.0"
    def log_message(self,*args): pass
    def send_json(self,status,data):
        raw=json.dumps(data,separators=(",",":"),default=str).encode()
        self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(raw))); self.send_header("Access-Control-Allow-Origin",os.getenv("CORS_ORIGIN","http://localhost:5173")); self.send_header("Access-Control-Allow-Headers","Content-Type"); self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS"); self.end_headers(); self.wfile.write(raw)
    def do_OPTIONS(self): self.send_response(204); self.send_header("Access-Control-Allow-Origin",os.getenv("CORS_ORIGIN","http://localhost:5173")); self.send_header("Access-Control-Allow-Headers","Content-Type"); self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS"); self.end_headers()
    def do_POST(self):
        if self.path.split("?")[0]!="/api/telemetry": return self.send_json(404,{"error":"Not found"})
        try:
            n=int(self.headers.get("Content-Length","0"))
            if not 1 <= n <= 2_000_000: return self.send_json(413,{"error":"Payload must be between 1 byte and 2 MB"})
            body=json.loads(self.rfile.read(n)); events=body.get("events") if isinstance(body,dict) and "events" in body else [body]
            if not isinstance(events,list) or not 1 <= len(events) <= 500: return self.send_json(400,{"error":"Send 1 to 500 telemetry events"})
            accepted=duplicates=0; made=[]
            for raw in events:
                event=validate_event(raw); ok,alerts=store.ingest(event)
                accepted+=int(ok); duplicates+=int(not ok)
                if ok:
                    for alert in alerts: made.append({**alert,"vehicle_id":event["vehicle_id"],"timestamp":event["timestamp"]}); publish("alert",made[-1])
                    publish("telemetry",{"vehicle_id":event["vehicle_id"],"speed_kmh":event["speed_kmh"],"fuel_level_pct":event["fuel_level_pct"],"idle":event["idle"],"timestamp":event["timestamp"]})
            publish("summary",store.summary())
            return self.send_json(202,{"accepted":accepted,"duplicates":duplicates,"alerts_created":len(made)})
        except ValidationError as e: return self.send_json(422,{"error":str(e)})
        except ValueError as e: return self.send_json(422,{"error":str(e)})
        except (json.JSONDecodeError,UnicodeDecodeError): return self.send_json(400,{"error":"Invalid JSON"})
        except Exception: return self.send_json(500,{"error":"Telemetry processing failed"})
    def do_GET(self):
        parsed=urlparse(self.path); path=parsed.path; q=parse_qs(parsed.query)
        try:
            if path=="/api/events":
                sub=queue.Queue(50)
                with sub_lock: subscribers.add(sub)
                self.send_response(200); self.send_header("Content-Type","text/event-stream"); self.send_header("Cache-Control","no-cache"); self.send_header("Access-Control-Allow-Origin",os.getenv("CORS_ORIGIN","http://localhost:5173")); self.end_headers()
                self.wfile.write(("data: "+json.dumps({"type":"summary","data":store.summary()})+"\n\n").encode()); self.wfile.flush()
                try:
                    while True:
                        try: payload=sub.get(timeout=20); msg="data: "+payload+"\n\n"
                        except queue.Empty: msg=": keepalive\n\n"
                        self.wfile.write(msg.encode()); self.wfile.flush()
                except (BrokenPipeError,ConnectionResetError): pass
                finally:
                    with sub_lock: subscribers.discard(sub)
                return
            if path=="/api/health": return self.send_json(200,{"status":"ok","service":"fleetpulse-api","vehicles":store.summary()['total_vehicles']})
            if path=="/api/dashboard/summary": return self.send_json(200,store.summary())
            if path=="/api/vehicles": return self.send_json(200,store.vehicles(self.integer(q,"page",1,1,1_000_000),self.integer(q,"page_size",25,1,100),q.get("search",[""])[0][:80],q.get("status",[""])[0]))
            if path.startswith("/api/vehicles/"):
                row=store.vehicle(path.rsplit("/",1)[-1]); return self.send_json(200,row) if row else self.send_json(404,{"error":"Vehicle not found"})
            if path=="/api/alerts": return self.send_json(200,store.alerts(self.integer(q,"page",1,1,1_000_000),self.integer(q,"page_size",25,1,100),q.get("severity",[""])[0]))
            if path=="/api/analytics/fuel": return self.send_json(200,store.fuel(self.integer(q,"days",7,1,90)))
            if path=="/api/analytics/utilisation": return self.send_json(200,store.utilisation(self.integer(q,"days",7,1,90)))
            return self.send_json(404,{"error":"Not found"})
        except ValueError as e: return self.send_json(400,{"error":str(e)})
        except Exception: return self.send_json(500,{"error":"Request failed"})
    @staticmethod
    def integer(q,key,default,lo,hi):
        try: x=int(q.get(key,[default])[0])
        except (ValueError,TypeError): raise ValueError(f"{key} must be an integer")
        if not lo<=x<=hi: raise ValueError(f"{key} must be between {lo} and {hi}")
        return x

if __name__=="__main__":
    host=os.getenv("HOST","127.0.0.1"); port=int(os.getenv("PORT","8000"))
    print(f"FleetPulse API listening on http://{host}:{port} ({store.summary()['total_vehicles']:,} virtual vehicles)")
    ThreadingHTTPServer((host,port),Handler).serve_forever()
