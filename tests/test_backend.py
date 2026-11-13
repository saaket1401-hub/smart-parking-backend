import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from backend import config
from backend.main import create_app

TOPIC = "uts31261/fajar/parking/{}/status"


def msg(spot=1001, occupied=False, ctr=0, hb=True, batt=3599):
    return json.dumps({"spot_id": spot, "occupied": occupied, "batt_mv": batt,
                       "status": 1 if occupied else 0, "ctr": ctr, "heartbeat": hb})


@pytest.fixture
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "t.db"), mqtt_enabled=False, watchdog=False)
    with TestClient(app) as c:
        c.app = app
        yield c


def post(c, body, spot=1001):
    return c.post(f"/api/ingest?topic={TOPIC.format(spot)}", content=body)


def test_initial_state_unknown(client):
    data = client.get("/api/spaces").json()
    assert [s["space_id"] for s in data["spaces"]] == ["A01", "A02", "A03"]
    assert data["summary"] == {"Available": 0, "Occupied": 0, "Unknown": 3}


def test_state_change_is_stored_with_history(client):
    assert post(client, msg(ctr=0)).status_code == 200
    assert post(client, msg(occupied=True, ctr=1, hb=False)).status_code == 200
    s = client.get("/api/spaces/A01").json()
    assert s["status"] == "Occupied" and s["online"] == 1 and s["last_ctr"] == 1
    ev = client.get("/api/spaces/A01/history").json()["events"]
    assert [(e["old_status"], e["new_status"], e["source"]) for e in ev] == [
        ("Available", "Occupied", "sensor"), ("Unknown", "Available", "sensor")]
    assert all(e["event_time"].endswith("+00:00") for e in ev)


def test_heartbeat_does_not_add_history(client):
    for i in range(4):
        post(client, msg(ctr=i))
    assert len(client.get("/api/spaces/A01/history").json()["events"]) == 1


@pytest.mark.parametrize("body,reason", [
    ("not json", "invalid JSON"),
    (msg(spot=9999), "not a registered bay"),
    (json.dumps({"spot_id": 1001}), "missing field"),
    (json.dumps({"spot_id": 1001, "occupied": True, "batt_mv": 3500, "status": 0, "ctr": 1,
                 "heartbeat": False}), "bit0"),
    (json.dumps({"spot_id": 1001, "occupied": "yes", "batt_mv": 3500, "status": 1, "ctr": 1,
                 "heartbeat": False}), "true/false"),
])
def test_invalid_messages_rejected_and_logged(client, body, reason):
    r = post(client, body)
    assert r.status_code == 422 and reason in r.json()["detail"]
    assert reason in client.get("/api/errors").json()["errors"][0]["reason"]


def test_topic_spot_mismatch_rejected(client):
    r = post(client, msg(spot=1001), spot=1002)
    assert r.status_code == 422 and "does not match" in r.json()["detail"]


def test_duplicate_and_lost_messages(client):
    post(client, msg(ctr=5))
    assert post(client, msg(ctr=5)).json()["duplicate"] is True
    r = post(client, msg(ctr=8)).json()          # 6 and 7 missing
    assert r["lost_messages"] == 2
    assert client.get("/api/spaces/A01").json()["lost_messages"] == 2


def test_counter_wrap_is_not_loss(client):
    post(client, msg(ctr=255))
    assert post(client, msg(ctr=0)).json()["lost_messages"] == 0


def test_heartbeat_timeout_sets_unknown_and_recovers(client):
    post(client, msg(occupied=True, ctr=0, hb=False))
    proc = client.app.state.processor
    future = datetime.now(timezone.utc) + timedelta(seconds=config.HEARTBEAT_TIMEOUT_S + 1)
    assert proc.check_timeouts(now=future) == ["A01"]
    s = client.get("/api/spaces/A01").json()
    assert s["status"] == "Unknown" and s["online"] == 0
    assert client.get("/api/spaces/A01/history").json()["events"][0]["source"] == "timeout"
    post(client, msg(occupied=True, ctr=1))        # node comes back
    s = client.get("/api/spaces/A01").json()
    assert s["status"] == "Occupied" and s["online"] == 1


def test_admin_override_requires_token_and_holds(client):
    post(client, msg(ctr=0))
    url = "/api/admin/spaces/A01/override"
    assert client.post(url, json={"status": "Occupied"}).status_code == 401
    h = {"X-Admin-Token": config.ADMIN_TOKEN}
    assert client.post(url, json={"status": "Occupied"}, headers=h).json()["status"] == "Occupied"
    post(client, msg(ctr=1))                       # sensor says free, override wins
    assert client.get("/api/spaces/A01").json()["status"] == "Occupied"
    assert client.delete(url, headers=h).json()["status"] == "Available"
    sources = [e["source"] for e in client.get("/api/spaces/A01/history").json()["events"]]
    assert sources[:2] == ["manual_clear", "manual"]


def test_websocket_snapshot_and_push(client):
    with client.websocket_connect("/ws") as ws:
        snap = json.loads(ws.receive_text())
        assert snap["type"] == "snapshot" and len(snap["spaces"]) == 3
        post(client, msg(spot=1002, occupied=True, ctr=0, hb=False), spot=1002)
        upd = json.loads(ws.receive_text())
        assert upd["type"] == "space_update"
        assert upd["space"]["space_id"] == "A02" and upd["space"]["status"] == "Occupied"


def test_web_app_is_served(client):
    r = client.get("/app/")
    assert r.status_code == 200 and "Smart Parking System" in r.text
    assert "fetchSpaces" in client.get("/app/app.js").text
