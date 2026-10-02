"""Bounded async simulator sampling from a 100k-vehicle virtual fleet."""
import asyncio, json, os, random, time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError
from uuid import uuid4

COUNT=int(os.getenv("VEHICLE_COUNT","100000")); RATE=float(os.getenv("EVENTS_PER_SECOND","10")); API=os.getenv("API_URL","http://127.0.0.1:8000/api/telemetry"); SEED=int(os.getenv("SIMULATOR_SEED","42"))
TYPES=["Van","Truck","Pickup","Coach"]

def make_event(i,rng,state):
    kind=i%100
    engine=kind not in (0,1,2,3)
    idle=engine and kind in (4,5,6,7,8,9,10,11,12,13,14,15)
    speed=0 if idle or not engine else rng.uniform(15,82)
    fuel_rate=(rng.uniform(2.2,3.8) if i%4 in (0,2) else rng.uniform(4.5,8.0)) if idle else (rng.uniform(1.0,2.4) if engine else 0)
    state["odo"]+=speed/360
    state["fuel"] = max(3,min(100,state["fuel"]-fuel_rate/360*100/80+rng.uniform(-.02,.02)))
    # Rare noisy location/fuel values remain inside API validation bounds.
    lat=12.8+(i%900)/1000+rng.uniform(-.002,.002); lon=77.6+(i%1200)/1000+rng.uniform(-.002,.002)
    event={"event_id":str(uuid4()),"vehicle_id":f"VH-{i:06d}","timestamp":datetime.now(timezone.utc).isoformat(),"latitude":round(lat,6),"longitude":round(lon,6),"speed_kmh":round(speed,1),"fuel_level_pct":round(state["fuel"],2),"fuel_rate_lph":round(fuel_rate,2),"odometer_km":round(state["odo"],1),"engine_on":engine,"idle":idle}
    # A few events represent a long idle already in progress so a short demo
    # can exercise the real-time critical alert rule without waiting 30 min.
    if idle and i%500==4: event["idle_duration_minutes"]=rng.randint(30,48)
    return event

def send(events):
    req=Request(API,data=json.dumps({"events":events}).encode(),headers={"Content-Type":"application/json"},method="POST")
    with urlopen(req,timeout=10) as res: return json.loads(res.read())

async def run(once=False):
    rng=random.Random(SEED); states={}; interval=min(1.0,100/max(1,RATE)); batch_size=max(1,min(100,int(RATE*interval)))
    print(f"Simulator online: {COUNT:,} virtual vehicles, target {RATE:g} events/sec -> {API}")
    while True:
        events=[]
        for _ in range(batch_size):
            i=rng.randint(1,COUNT); state=states.setdefault(i,{"odo":float((i*7919)%320000),"fuel":float(25+(i*31)%76)})
            events.append(make_event(i,rng,state))
        # Deliberate occasional duplicate and timestamp skew demonstrate ingest resilience.
        if events and rng.random()<.04: events.append(events[0])
        if len(events)>2 and rng.random()<.04: events[1]["timestamp"]="2024-01-01T00:00:00+00:00"
        try: print(send(events))
        except (URLError,TimeoutError) as e: print(f"API unavailable: {e}")
        if once: return
        await asyncio.sleep(interval)

if __name__=="__main__":
    import sys
    asyncio.run(run("--once" in sys.argv))
