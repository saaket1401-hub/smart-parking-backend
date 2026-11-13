"""Message validation, normalisation and processing (Section 5.6).

Expected node message (Section 5.5):
  {"spot_id":1001,"occupied":true,"batt_mv":3599,"status":1,"ctr":4,"heartbeat":false}
"""
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Callable

from .db import Database, utcnow

STATUS_OCCUPIED = 0x01
STATUS_FAULT = 0x02
STATUS_LOW_BATT = 0x04

REQUIRED = {
    "spot_id": int,
    "occupied": bool,
    "batt_mv": int,
    "status": int,
    "ctr": int,
    "heartbeat": bool,
}

TOPIC_SPOT = re.compile(r"/parking/(\d+)/status$")


class ValidationError(ValueError):
    pass


def parse_and_validate(payload: str | bytes, topic: str | None, registered: set[int]) -> dict:
    """Return the validated message or raise ValidationError with a reason."""
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            raise ValidationError("payload is not UTF-8")
    try:
        msg = json.loads(payload)
    except json.JSONDecodeError as e:
        raise ValidationError(f"invalid JSON: {e.msg}")
    if not isinstance(msg, dict):
        raise ValidationError("payload is not a JSON object")

    for field, typ in REQUIRED.items():
        if field not in msg:
            raise ValidationError(f"missing field '{field}'")
        val = msg[field]
        # bool is a subclass of int in Python, so check it explicitly
        if typ is int and (isinstance(val, bool) or not isinstance(val, int)):
            raise ValidationError(f"field '{field}' must be an integer")
        if typ is bool and not isinstance(val, bool):
            raise ValidationError(f"field '{field}' must be true/false")

    if msg["spot_id"] not in registered:
        raise ValidationError(f"spot_id {msg['spot_id']} is not a registered bay")
    if not 0 <= msg["status"] <= 0x07:
        raise ValidationError("status byte out of range (0-7)")
    if bool(msg["status"] & STATUS_OCCUPIED) != msg["occupied"]:
        raise ValidationError("status bit0 does not match 'occupied'")
    if not 0 <= msg["ctr"] <= 255:
        raise ValidationError("ctr out of range (0-255)")
    if not 0 <= msg["batt_mv"] <= 6000:
        raise ValidationError("batt_mv out of range (0-6000)")
    if topic:
        m = TOPIC_SPOT.search(topic)
        if m and int(m.group(1)) != msg["spot_id"]:
            raise ValidationError(f"topic spot {m.group(1)} does not match payload spot_id {msg['spot_id']}")
    return msg


class Processor:
    """Applies validated messages to the database and runs the heartbeat watchdog.

    `notify(space_dict)` is called whenever a bay's displayed state changes,
    so the API layer can push it to browsers over WebSocket.
    """

    def __init__(self, db: Database, spot_map: dict[int, str], heartbeat_timeout_s: float,
                 notify: Callable[[dict], None] | None = None):
        self.db = db
        self.spot_map = spot_map
        self.timeout = timedelta(seconds=heartbeat_timeout_s)
        self.notify = notify or (lambda _s: None)

    # ---------- incoming messages ----------
    def handle(self, payload: str | bytes, topic: str | None = None) -> dict:
        """Process one raw message. Returns a short result dict (also used by the HTTP ingest route)."""
        raw = payload.decode("utf-8", "replace") if isinstance(payload, bytes) else payload
        try:
            msg = parse_and_validate(payload, topic, set(self.spot_map))
        except ValidationError as e:
            self.db.log_error(topic, raw, str(e))
            return {"accepted": False, "reason": str(e)}

        spot_id = msg["spot_id"]
        space_id = self.spot_map[spot_id]
        now = utcnow()

        with self.db.lock:
            node = self.db.one("SELECT * FROM sensor_node WHERE node_id=?", (spot_id,))
            last_ctr = node["last_ctr"] if node else None
            if last_ctr is not None and msg["ctr"] == last_ctr:
                # duplicate (e.g. QoS 1 redelivery): refresh liveness only
                self.db.conn.execute("UPDATE sensor_node SET last_seen=?, online=1 WHERE node_id=?",
                                     (now, spot_id))
                self.db.conn.commit()
                return {"accepted": True, "duplicate": True, "space_id": space_id}

            lost = 0
            if last_ctr is not None:
                gap = (msg["ctr"] - last_ctr - 1) % 256
                # a very large "gap" usually means the node rebooted (ctr reset to 0)
                if 0 < gap < 128:
                    lost = gap

            was_online = bool(node["online"]) if node else False
            self.db.conn.execute(
                """UPDATE sensor_node SET online=1, last_seen=?, batt_mv=?, status_flags=?,
                       last_ctr=?, lost_messages=lost_messages+?, reported_occupied=?
                   WHERE node_id=?""",
                (now, msg["batt_mv"], msg["status"], msg["ctr"], lost, int(msg["occupied"]), spot_id))
            self.db.conn.commit()
            if lost:
                self.db.log_error(topic, raw, f"{lost} message(s) lost before ctr {msg['ctr']}")

            override = self.db.one("SELECT manual_override FROM space_status WHERE space_id=?",
                                   (space_id,))["manual_override"]
            changed = False
            if not override:
                new_status = "Occupied" if msg["occupied"] else "Available"
                changed = self.db.set_status(space_id, new_status, "sensor")

        if changed or not was_online:
            self.notify(self.db.space(space_id))
        return {"accepted": True, "duplicate": False, "space_id": space_id,
                "status_changed": changed, "lost_messages": lost}

    # ---------- heartbeat watchdog ----------
    def check_timeouts(self, now: datetime | None = None) -> list[str]:
        """Mark nodes offline (and their bays Unknown) after the heartbeat timeout."""
        now = now or datetime.now(timezone.utc)
        cutoff = (now - self.timeout).isoformat(timespec="milliseconds")
        timed_out = []
        with self.db.lock:
            rows = self.db.query(
                "SELECT node_id, space_id FROM sensor_node WHERE online=1 AND last_seen < ?",
                (cutoff,))
            for r in rows:
                self.db.conn.execute("UPDATE sensor_node SET online=0 WHERE node_id=?", (r["node_id"],))
                self.db.conn.commit()
                override = self.db.one("SELECT manual_override FROM space_status WHERE space_id=?",
                                       (r["space_id"],))["manual_override"]
                if not override:
                    self.db.set_status(r["space_id"], "Unknown", "timeout")
                timed_out.append(r["space_id"])
        for space_id in timed_out:
            self.notify(self.db.space(space_id))
        return timed_out

    # ---------- administrator override ----------
    def override(self, space_id: str, status: str) -> dict:
        self.db.set_status(space_id, status, "manual", manual_override=1)
        s = self.db.space(space_id)
        self.notify(s)
        return s

    def clear_override(self, space_id: str) -> dict:
        """Return the bay to sensor control, using the sensor's last report."""
        with self.db.lock:
            node = self.db.one("SELECT online, reported_occupied FROM sensor_node WHERE space_id=?",
                               (space_id,))
            if node and node["online"] and node["reported_occupied"] is not None:
                status = "Occupied" if node["reported_occupied"] else "Available"
            else:
                status = "Unknown"
            self.db.set_status(space_id, status, "manual_clear", manual_override=0)
        s = self.db.space(space_id)
        self.notify(s)
        return s
