"""SQLite storage: the four tables from Section 5.6 plus an error log."""
import sqlite3
import threading
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS parking_space (
    space_id TEXT PRIMARY KEY,
    zone     TEXT NOT NULL,
    spot_id  INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS sensor_node (
    node_id   INTEGER PRIMARY KEY,          -- the node's numeric spot_id
    space_id  TEXT NOT NULL REFERENCES parking_space(space_id),
    online    INTEGER NOT NULL DEFAULT 0,
    last_seen TEXT,
    batt_mv   INTEGER,
    status_flags INTEGER,
    last_ctr  INTEGER,
    lost_messages INTEGER NOT NULL DEFAULT 0,
    reported_occupied INTEGER              -- last state the sensor reported
);

CREATE TABLE IF NOT EXISTS space_status (
    space_id   TEXT PRIMARY KEY REFERENCES parking_space(space_id),
    status     TEXT NOT NULL CHECK (status IN ('Available','Occupied','Unknown')),
    updated_at TEXT NOT NULL,
    manual_override INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS status_event (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    space_id   TEXT NOT NULL REFERENCES parking_space(space_id),
    old_status TEXT,
    new_status TEXT NOT NULL,
    event_time TEXT NOT NULL,
    source     TEXT NOT NULL              -- sensor | timeout | manual | manual_clear
);

CREATE TABLE IF NOT EXISTS message_error (
    error_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at TEXT NOT NULL,
    topic       TEXT,
    payload     TEXT,
    reason      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_space ON status_event(space_id, event_id);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Database:
    """Thread-safe wrapper: MQTT callbacks, the watchdog and HTTP requests
    all write through one connection guarded by a lock."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.lock = threading.RLock()
        with self.lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def register_spaces(self, spot_map: dict[int, str], zone: str) -> None:
        now = utcnow()
        with self.lock:
            for spot_id, space_id in spot_map.items():
                self.conn.execute(
                    "INSERT OR IGNORE INTO parking_space(space_id, zone, spot_id) VALUES (?,?,?)",
                    (space_id, zone, spot_id))
                self.conn.execute(
                    "INSERT OR IGNORE INTO sensor_node(node_id, space_id) VALUES (?,?)",
                    (spot_id, space_id))
                self.conn.execute(
                    "INSERT OR IGNORE INTO space_status(space_id, status, updated_at) VALUES (?,?,?)",
                    (space_id, "Unknown", now))
            self.conn.commit()

    # ---------- reads ----------
    def query(self, sql: str, args: tuple = ()) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def one(self, sql: str, args: tuple = ()) -> dict | None:
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def spaces(self) -> list[dict]:
        return self.query("""
            SELECT p.space_id, p.zone, p.spot_id, s.status, s.updated_at, s.manual_override,
                   n.online, n.last_seen, n.batt_mv, n.status_flags, n.last_ctr, n.lost_messages
            FROM parking_space p
            JOIN space_status s ON s.space_id = p.space_id
            JOIN sensor_node  n ON n.space_id = p.space_id
            ORDER BY p.space_id""")

    def space(self, space_id: str) -> dict | None:
        rows = [r for r in self.spaces() if r["space_id"] == space_id]
        return rows[0] if rows else None

    def nodes(self) -> list[dict]:
        return self.query("SELECT * FROM sensor_node ORDER BY node_id")

    def history(self, space_id: str, limit: int = 100) -> list[dict]:
        return self.query(
            "SELECT * FROM status_event WHERE space_id=? ORDER BY event_id DESC LIMIT ?",
            (space_id, limit))

    def errors(self, limit: int = 100) -> list[dict]:
        return self.query("SELECT * FROM message_error ORDER BY error_id DESC LIMIT ?", (limit,))

    # ---------- writes ----------
    def log_error(self, topic: str | None, payload: str, reason: str) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO message_error(received_at, topic, payload, reason) VALUES (?,?,?,?)",
                (utcnow(), topic, payload[:2000], reason))
            self.conn.commit()

    def set_status(self, space_id: str, new_status: str, source: str,
                   manual_override: int | None = None) -> bool:
        """Update space_status and append to status_event if the status changed.
        Returns True when the displayed status changed."""
        now = utcnow()
        with self.lock:
            row = self.conn.execute(
                "SELECT status, manual_override FROM space_status WHERE space_id=?",
                (space_id,)).fetchone()
            old = row["status"] if row else None
            mo = row["manual_override"] if manual_override is None else manual_override
            changed = old != new_status
            self.conn.execute(
                "UPDATE space_status SET status=?, updated_at=?, manual_override=? WHERE space_id=?",
                (new_status, now if changed else (self._updated_at(space_id) or now), mo, space_id))
            if changed:
                self.conn.execute(
                    "INSERT INTO status_event(space_id, old_status, new_status, event_time, source) "
                    "VALUES (?,?,?,?,?)", (space_id, old, new_status, now, source))
            self.conn.commit()
            return changed

    def _updated_at(self, space_id: str) -> str | None:
        r = self.conn.execute("SELECT updated_at FROM space_status WHERE space_id=?",
                              (space_id,)).fetchone()
        return r["updated_at"] if r else None
