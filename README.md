# FleetPulse

FleetPulse is a local, end-to-end connected fleet intelligence demo. A Python simulator models 100,000 vehicles without spawning per-vehicle processes, posts telemetry batches to a Python API, and the API validates, deduplicates, processes, and persists events in SQLite. Server-sent events deliver new alerts and live metrics to a React dashboard.

## Architecture

- **API and processing:** Python standard library HTTP server. The modular monolith keeps ingestion, rules, storage, and API easy to run on a laptop.
- **Business state and telemetry:** SQLite stores vehicles, idempotency keys, recent telemetry, daily aggregates, and alerts. Recent telemetry is capped; daily aggregates keep history compact. SQLite is the practical local MVP choice; migrate the telemetry repository to partitioned PostgreSQL/Timescale or a durable stream plus columnar object storage when write volume or retention requires it.
- **Historical analytics:** pre-aggregated daily rollups are queried independently from per-event alert rules.
- **Live updates:** Server-Sent Events (SSE), appropriate for one-way API-to-dashboard updates.
- **Simulator:** deterministic seeded vehicle attributes and bounded batches. Every tick samples from the 100k virtual fleet; event rate is controlled independently.
- No vector database is used: the problem is numeric/time-series analytics and has no semantic retrieval requirement.

## Run locally

Requirements: Python 3.10+, Node.js 18+ and pnpm (or npm).

1. `python -m backend.server` (API on http://localhost:8000)
2. In another terminal: `python -m simulator.runner` (simulates 100,000 vehicles, sends 10 events/sec by default)
3. In another terminal: `pnpm install` then `pnpm dev` (dashboard on http://localhost:5173)

Set `VEHICLE_COUNT`, `EVENTS_PER_SECOND`, `API_URL`, `DATABASE_PATH`, and `SIMULATOR_SEED` to configure the demo. The server seeds the virtual vehicle directory on first start. Stop the simulator to pause incoming events; the API and persisted dashboard remain available.

## API

- `GET /api/health`
- `GET /api/dashboard/summary`
- `GET /api/vehicles?page=1&page_size=25&search=&status=`
- `GET /api/vehicles/{vehicle_id}`
- `GET /api/alerts?page=1&page_size=25&severity=`
- `GET /api/analytics/fuel?days=7`
- `GET /api/analytics/utilisation?days=7`
- `POST /api/telemetry` accepts one event or `{ "events": [...] }`
- `GET /api/events` streams `telemetry`, `alert`, and `summary` event payloads over SSE.


## Verification

Run `python -m unittest discover -s tests`. Start the service and simulator as above, then open the dashboard. Use `python -m simulator.runner --once` to send a deterministic sample batch.
