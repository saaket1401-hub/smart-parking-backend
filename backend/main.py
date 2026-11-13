"""FastAPI application: REST API, WebSocket push and the web application.

Run:  uvicorn backend.main:app --host 0.0.0.0 --port 8000
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config
from .db import Database, utcnow
from .ingest import Processor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("parking.api")

WEB_DIR = Path(__file__).parent / "static" / "web"


class Hub:
    """Keeps the open WebSocket connections and broadcasts bay updates.
    notify() may be called from the MQTT thread, so it hops onto the event loop."""

    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.loop: asyncio.AbstractEventLoop | None = None

    def notify(self, space: dict | None) -> None:
        if not space or not self.loop:
            return
        msg = json.dumps({"type": "space_update", "space": space})
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self.loop:
            self.loop.create_task(self._broadcast(msg))
        else:
            asyncio.run_coroutine_threadsafe(self._broadcast(msg), self.loop)

    async def _broadcast(self, msg: str) -> None:
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


class OverrideIn(BaseModel):
    status: str


def create_app(db_path: str | None = None, mqtt_enabled: bool | None = None,
               watchdog: bool = True) -> FastAPI:
    db = Database(db_path or config.DB_PATH)
    db.register_spaces(config.SPOT_MAP, config.ZONE)
    hub = Hub()
    processor = Processor(db, config.SPOT_MAP, config.HEARTBEAT_TIMEOUT_S, notify=hub.notify)
    use_mqtt = config.MQTT_ENABLED if mqtt_enabled is None else mqtt_enabled
    state = {"mqtt": None}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        hub.loop = asyncio.get_running_loop()
        if use_mqtt:
            from .mqtt_client import MqttIngest
            state["mqtt"] = MqttIngest(processor, config.MQTT_HOST, config.MQTT_PORT, config.MQTT_TOPIC)
            state["mqtt"].start()
        task = None
        if watchdog:
            async def run_watchdog():
                while True:
                    await asyncio.sleep(config.WATCHDOG_INTERVAL_S)
                    processor.check_timeouts()
            task = asyncio.create_task(run_watchdog())
        yield
        if task:
            task.cancel()
        if state["mqtt"]:
            state["mqtt"].stop()

    app = FastAPI(title="Smart Parking Backend", version="1.0", lifespan=lifespan)
    app.state.db, app.state.processor, app.state.hub = db, processor, hub
    # Allow the GitHub Pages copy of the web app (or any origin) to read the API
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    def require_admin(token: str | None):
        if token != config.ADMIN_TOKEN:
            raise HTTPException(status_code=401, detail="invalid or missing X-Admin-Token")

    def get_space_or_404(space_id: str) -> dict:
        s = db.space(space_id)
        if not s:
            raise HTTPException(status_code=404, detail=f"unknown space {space_id}")
        return s

    @app.get("/api/health")
    def health():
        m = state["mqtt"]
        return {"ok": True, "time": utcnow(),
                "mqtt": {"enabled": use_mqtt, "connected": bool(m and m.connected),
                         "host": config.MQTT_HOST, "topic": config.MQTT_TOPIC,
                         "messages_received": m.received if m else 0},
                "heartbeat_timeout_s": config.HEARTBEAT_TIMEOUT_S}

    @app.get("/api/spaces")
    def spaces():
        rows = db.spaces()
        counts = {k: sum(r["status"] == k for r in rows) for k in ("Available", "Occupied", "Unknown")}
        return {"spaces": rows, "summary": counts}

    @app.get("/api/spaces/{space_id}")
    def space(space_id: str):
        return get_space_or_404(space_id)

    @app.get("/api/spaces/{space_id}/history")
    def history(space_id: str, limit: int = 100):
        get_space_or_404(space_id)
        return {"space_id": space_id, "events": db.history(space_id, max(1, min(limit, 1000)))}

    @app.get("/api/nodes")
    def nodes():
        return {"nodes": db.nodes()}

    @app.get("/api/errors")
    def errors(limit: int = 100):
        return {"errors": db.errors(max(1, min(limit, 1000)))}

    @app.post("/api/ingest")
    async def ingest(request: Request, topic: str | None = None):
        """HTTP alternative to MQTT (for a gateway, or testing without a broker)."""
        body = await request.body()
        result = processor.handle(body, topic)
        if not result["accepted"]:
            raise HTTPException(status_code=422, detail=result["reason"])
        return result

    @app.post("/api/admin/spaces/{space_id}/override")
    def override(space_id: str, body: OverrideIn, x_admin_token: str | None = Header(default=None)):
        require_admin(x_admin_token)
        get_space_or_404(space_id)
        if body.status not in ("Available", "Occupied", "Unknown"):
            raise HTTPException(status_code=422, detail="status must be Available, Occupied or Unknown")
        return processor.override(space_id, body.status)

    @app.delete("/api/admin/spaces/{space_id}/override")
    def clear_override(space_id: str, x_admin_token: str | None = Header(default=None)):
        require_admin(x_admin_token)
        get_space_or_404(space_id)
        return processor.clear_override(space_id)

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        hub.clients.add(websocket)
        try:
            await websocket.send_text(json.dumps({"type": "snapshot", "spaces": db.spaces()}))
            while True:
                await websocket.receive_text()   # keep-alive; client messages are ignored
        except WebSocketDisconnect:
            pass
        finally:
            hub.clients.discard(websocket)

    if WEB_DIR.exists():
        app.mount("/app", StaticFiles(directory=WEB_DIR, html=True), name="web")

        @app.get("/", include_in_schema=False)
        def root():
            return RedirectResponse("/app/")

    return app


app = create_app()
