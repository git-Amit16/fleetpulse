"""Explainable event validation and alert rules."""
from datetime import datetime, timezone
from math import isfinite
from uuid import UUID

class ValidationError(ValueError):
    pass

def validate_event(event):
    required = {"event_id", "vehicle_id", "timestamp", "latitude", "longitude", "speed_kmh", "fuel_level_pct", "fuel_rate_lph", "odometer_km", "engine_on", "idle"}
    if not isinstance(event, dict) or not required.issubset(event):
        raise ValidationError("Missing required telemetry fields")
    try:
        UUID(str(event["event_id"]))
        dt = datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        raise ValidationError("event_id must be a UUID and timestamp must be ISO-8601") from None
    if dt.tzinfo is None:
        raise ValidationError("timestamp must include a timezone")
    if not isinstance(event["vehicle_id"], str) or not 1 <= len(event["vehicle_id"]) <= 40:
        raise ValidationError("Invalid vehicle_id")
    bounds = {"latitude": (-90, 90), "longitude": (-180, 180), "speed_kmh": (0, 220), "fuel_level_pct": (0, 100), "fuel_rate_lph": (0, 200), "odometer_km": (0, 5_000_000)}
    for key, (lo, hi) in bounds.items():
        value = event[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or not lo <= value <= hi:
            raise ValidationError(f"{key} is outside its accepted range")
    if not isinstance(event["engine_on"], bool) or not isinstance(event["idle"], bool):
        raise ValidationError("engine_on and idle must be booleans")
    if event["idle"] and (not event["engine_on"] or event["speed_kmh"] > 3):
        raise ValidationError("idle telemetry requires engine on and speed at most 3 km/h")
    if "idle_duration_minutes" in event and (isinstance(event["idle_duration_minutes"], bool) or not isinstance(event["idle_duration_minutes"], int) or not 0 <= event["idle_duration_minutes"] <= 1440 or not event["idle"]):
        raise ValidationError("idle_duration_minutes must be an integer from 0 to 1440 on idle telemetry")
    return event

def detect_alerts(event, vehicle, idle_seconds=0):
    alerts = []
    if event["idle"] and idle_seconds >= 30 * 60:
        waste_l = event["fuel_rate_lph"] * (idle_seconds / 3600)
        alerts.append({"type": "excessive_idling", "severity": "CRITICAL", "description": f"Engine idling for {idle_seconds // 60} minutes; estimated fuel waste {waste_l:.1f} L.", "estimated_cost_inr": round(waste_l * 100, 2)})
    expected = vehicle.get("expected_l_per_100km", 8.2)
    actual = vehicle.get("recent_l_per_100km", 0)
    if actual > expected * 1.12:
        pct = round((actual / expected - 1) * 100)
        alerts.append({"type": "fuel_anomaly", "severity": "WARNING", "description": f"Fuel consumption {actual:.1f} L/100km vs expected {expected:.1f} (+{pct}%).", "estimated_cost_inr": None})
    if vehicle.get("utilisation_pct", 100) < 35:
        alerts.append({"type": "low_utilisation", "severity": "WARNING", "description": f"Vehicle utilisation is {vehicle['utilisation_pct']:.0f}%, below the 35% fleet threshold.", "estimated_cost_inr": None})
    return alerts
