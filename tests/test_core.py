import json, tempfile, unittest, os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from uuid import uuid4
from backend.processing import ValidationError, validate_event, detect_alerts
from backend.storage import Store

def event(vehicle="VH-000001", **overrides):
    data={"event_id":str(uuid4()),"vehicle_id":vehicle,"timestamp":datetime.now(timezone.utc).isoformat(),"latitude":13.08,"longitude":80.27,"speed_kmh":0,"fuel_level_pct":62.4,"fuel_rate_lph":2.8,"odometer_km":48291.4,"engine_on":True,"idle":True}
    return data|overrides

class CoreTests(unittest.TestCase):
    def test_schema_validates_and_rejects_bounds(self):
        self.assertEqual(validate_event(event())["vehicle_id"],"VH-000001")
        with self.assertRaises(ValidationError): validate_event(event(speed_kmh=300))

    def test_idling_alert(self):
        alerts=detect_alerts(event(),{"expected_l_per_100km":8.2,"utilisation_pct":80},2400)
        self.assertEqual(alerts[0]["severity"],"CRITICAL")
        self.assertGreater(alerts[0]["estimated_cost_inr"],0)

    def test_fuel_anomaly_and_low_utilisation(self):
        alerts=detect_alerts(event(),{"expected_l_per_100km":8.2,"recent_l_per_100km":10.0,"utilisation_pct":20})
        self.assertEqual({a["type"] for a in alerts},{"fuel_anomaly","low_utilisation"})

    def test_duplicate_idempotency_and_utilisation(self):
        with tempfile.TemporaryDirectory() as folder:
            db=Store(os.path.join(folder,"test.db")); db.seed(2)
            e=event()
            self.assertTrue(db.ingest(e)[0]); self.assertFalse(db.ingest(e)[0])
            self.assertEqual(db.vehicles(page=1,size=1)["total"],2)
            self.assertIn("average_pct",db.utilisation())
            db.db.close()

    def test_concurrent_analytics_and_ingestion(self):
        with tempfile.TemporaryDirectory() as folder:
            db=Store(os.path.join(folder,"concurrent.db")); db.seed(4)
            def read_many(_):
                for _ in range(12):
                    db.summary(); db.vehicles(1,2); db.alerts(1,2); db.fuel(7); db.utilisation(7)
            def write_many(_):
                for _ in range(12): db.ingest(event())
            with ThreadPoolExecutor(max_workers=10) as pool:
                futures=[pool.submit(read_many,i) for i in range(8)]+[pool.submit(write_many,i) for i in range(2)]
                for future in futures: future.result()
            self.assertEqual(db.summary()["total_vehicles"],4)
            db.db.close()

if __name__=="__main__": unittest.main()
