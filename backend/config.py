"""Runtime configuration, read from environment variables.

Every value has a default that matches the project report, so the backend
runs with no configuration at all.
"""
import os


def _spot_map(raw: str) -> dict[int, str]:
    """Parse "1001:A01,1002:A02" into {1001: "A01", 1002: "A02"}."""
    out: dict[int, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        spot, space = item.split(":")
        out[int(spot)] = space.strip()
    return out


# MQTT: same public broker and topic as the Wokwi communication test (Section 5.5)
MQTT_ENABLED = os.getenv("MQTT_ENABLED", "1") == "1"
MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "uts31261/fajar/parking/+/status")

# Storage
DB_PATH = os.getenv("DB_PATH", "parking.db")

# Node health: three missed 5-second heartbeats -> offline / Unknown
HEARTBEAT_TIMEOUT_S = float(os.getenv("HEARTBEAT_TIMEOUT_S", "15"))
WATCHDOG_INTERVAL_S = float(os.getenv("WATCHDOG_INTERVAL_S", "1"))

# Registered bays: node spot_id -> space ID shown in the web application
SPOT_MAP = _spot_map(os.getenv("SPOT_MAP", "1001:A01,1002:A02,1003:A03"))
ZONE = os.getenv("ZONE", "A")

# Admin token for manual override (send as header X-Admin-Token)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "change-me")
