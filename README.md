# Smart Parking Backend

Backend and database for the 31261 IoT Smart Parking Availability System. It completes the
pipeline described in the report:

```
Wokwi ESP32 node ──MQTT──► broker ──► backend (validate, store) ──► SQLite
                                           │
                                           └──REST + WebSocket──► web application
```

What it does:

- **Subscribes** to `uts31261/fajar/parking/+/status`, the same broker (HiveMQ) and topic the Wokwi
  MQTT node already publishes to.
- **Validates** every message: JSON, required fields and types, registered spot ID, status byte
  matches `occupied`, value ranges, topic/payload spot ID match. Rejected messages go to an
  error log.
- **Checks the rolling counter**: drops duplicates, counts lost messages, handles the wrap at 256.
- **Stores** data in SQLite: `parking_space`, `sensor_node`, `space_status`, `status_event`
  (history with UTC timestamps) and `message_error`.
- **Marks a bay Unknown** when its node misses heartbeats for 15 s (three 5 s heartbeats), and
  recovers when messages resume.
- **Serves a REST API and a WebSocket**, which pushes every change to the browser.
- **Serves the web application** (Hyunjin's dashboard and admin view) at `/app/`, now showing
  live data.
- **Admin manual override**, protected by a token.

## Run it on GitHub (Codespaces)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/saaket1401-hub/smart-parking-backend)

Click the badge (or **Code → Codespaces → Create codespace on main**). When the codespace starts it
installs the requirements, starts the backend on port 8000 and starts the node simulator, which sends
data to the backend over HTTP. The **Ports** tab shows "Parking dashboard" on port 8000. Open it to
see the live web app, and add `/docs` to the address for the API documentation. The backend also
subscribes to HiveMQ, so a running Wokwi MQTT node shows up too.

- Logs: `tail -f /tmp/backend.log` and `tail -f /tmp/simulator.log` in the codespace terminal
- Restart: `pkill -f uvicorn; pkill -f simulate.py`, then re-run the `postStartCommand` from
  `.devcontainer/devcontainer.json`
- The forwarded port is private (only you, signed in to GitHub) unless you set it to Public in the
  Ports tab. Stop the codespace when you're done to save your free hours.

## Run on your own computer

### Setup

Python 3.10 or newer.

```bash
cd smart-parking-backend
python -m venv .venv
# Windows: .venv\Scripts\activate     macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

### Run

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000**. The page shows **"Live Data (Backend)"** in the header. All bays
show Unknown until a node sends a message.

Then send data using either of these:

- **Wokwi (real node):** start the MQTT version of the Wokwi project. Its messages for spot
  1001 appear on bay A01.
- **Simulator (no Wokwi needed):** in a second terminal run `python simulate.py`. This publishes
  bays 1001–1003 to HiveMQ with the same cycle and message format as the node. Add `--fast`
  for 10x speed.

To test the Unknown state, stop the Wokwi simulation, or run
`python simulate.py --silent-after 30`. About 15 s later the bay turns grey (Unknown) and the
admin view shows Offline.

#### No internet or broker?

Run `python simulate.py --http http://localhost:8000` to POST the same messages to
`/api/ingest` instead of MQTT. You can also start a local Mosquitto and run with
`MQTT_HOST=localhost`.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `MQTT_HOST` / `MQTT_PORT` | `broker.hivemq.com` / `1883` | MQTT broker |
| `MQTT_TOPIC` | `uts31261/fajar/parking/+/status` | Subscription topic |
| `MQTT_ENABLED` | `1` | Set `0` to use HTTP ingest only |
| `DB_PATH` | `parking.db` | SQLite file |
| `HEARTBEAT_TIMEOUT_S` | `15` | Silence before a node is offline |
| `SPOT_MAP` | `1001:A01,1002:A02,1003:A03` | Node spot ID to bay ID |
| `ADMIN_TOKEN` | `change-me` | Token for override endpoints |

Set these on Windows PowerShell with `$env:MQTT_HOST="localhost"`, or on macOS/Linux with
`export MQTT_HOST=localhost`.

## API

Interactive docs: http://localhost:8000/docs

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | Backend and MQTT connection status, messages received |
| GET | `/api/spaces` | Current status of every bay, plus Available/Occupied/Unknown counts |
| GET | `/api/spaces/{id}` | One bay, e.g. `A01` |
| GET | `/api/spaces/{id}/history` | Event history (newest first) |
| GET | `/api/nodes` | Node health: online, last seen, battery, flags, counter, lost messages |
| GET | `/api/errors` | Rejected messages and detected message loss |
| POST | `/api/ingest?topic=...` | Send one node message over HTTP |
| POST | `/api/admin/spaces/{id}/override` | Body `{"status":"Occupied"}`, header `X-Admin-Token` |
| DELETE | `/api/admin/spaces/{id}/override` | Return the bay to sensor control |
| WS | `/ws` | Sends a `snapshot` on connect, then a `space_update` on every change |

## Web application

`backend/static/web/` is Hyunjin's web app (index.html, style.css, app.js). The HTML and CSS
are unchanged. `app.js` keeps all of the original code and adds a live mode:

- On load it calls `/api/spaces`. If the backend answers, the dashboard and admin view use
  backend data and update instantly over the WebSocket. If the WebSocket drops, the page
  polls every 5 s.
- If the backend can't be reached, the original local simulation runs exactly as before,
  and the header shows "Simulation Mode".
- The backend address is the serving server when the page is opened from `/app/`. Otherwise
  it is `http://localhost:8000`, or whatever you pass as `?api=` in the page URL. Example:
  `https://hxnwls.github.io/smart-parking-web/?api=http://localhost:8000` once the updated
  `app.js` is pushed to that repository.

## Database

`parking.db` can be opened with any SQLite viewer (e.g. DB Browser for SQLite) to capture the
latest-status table and event history for the report figures.

```bash
sqlite3 parking.db "SELECT * FROM space_status;"
sqlite3 parking.db "SELECT * FROM status_event ORDER BY event_id DESC LIMIT 20;"
```

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

15 tests cover:

- state changes and history
- heartbeats not creating history
- validation failures
- duplicates and lost messages, including the counter wrap
- heartbeat timeout and recovery
- admin override
- WebSocket push
- the web app being served
