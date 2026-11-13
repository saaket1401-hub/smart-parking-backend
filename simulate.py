"""Node simulator: publishes the same JSON messages as the Wokwi ESP32 node.

Use it to test the backend without Wokwi, or to add extra bays.

  python simulate.py                                   # bays 1001-1003 to HiveMQ
  python simulate.py --host localhost                  # local Mosquitto broker
  python simulate.py --http http://localhost:8000      # no broker: POST to /api/ingest
  python simulate.py --spots 1001 --silent-after 40    # node stops after 40 s (bay -> Unknown)
  python simulate.py --fast                            # 10x speed for quick demos

Each bay follows the node's mock cycle (Section 5.3): free for 20 s, occupied
until second 55, then free again, repeating every 70 s. Bays are offset so they
do not all change at once. A state-change message is sent on each change and a
heartbeat every 5 s, with a rolling counter and a battery that drops 1 mV per
heartbeat.
"""
import argparse
import json
import time
import urllib.request

CYCLE_S, OCC_FROM, OCC_TO = 70, 20, 55
HEARTBEAT_S = 5


def make_msg(spot, occupied, batt, ctr, heartbeat):
    status = (0x01 if occupied else 0) | (0x04 if batt < 3200 else 0)
    return {"spot_id": spot, "occupied": occupied, "batt_mv": batt,
            "status": status, "ctr": ctr % 256, "heartbeat": heartbeat}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="broker.hivemq.com")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--topic", default="uts31261/fajar/parking/{spot}/status")
    ap.add_argument("--http", help="send to this backend URL via HTTP instead of MQTT")
    ap.add_argument("--spots", type=int, nargs="+", default=[1001, 1002, 1003])
    ap.add_argument("--silent-after", type=float, default=None,
                    help="stop sending for the FIRST spot after this many seconds")
    ap.add_argument("--duration", type=float, default=None, help="exit after this many seconds")
    ap.add_argument("--fast", action="store_true", help="run the cycle 10x faster")
    args = ap.parse_args()
    speed = 10.0 if args.fast else 1.0

    if args.http:
        def send(spot, msg):
            req = urllib.request.Request(
                f"{args.http.rstrip('/')}/api/ingest?topic=" + args.topic.format(spot=spot),
                data=json.dumps(msg).encode(), headers={"Content-Type": "application/json"},
                method="POST")
            try:
                urllib.request.urlopen(req, timeout=5).read()
            except Exception as e:
                print("  send failed:", e)
    else:
        import paho.mqtt.client as mqtt
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.connect(args.host, args.port, keepalive=30)
        client.loop_start()

        def send(spot, msg):
            client.publish(args.topic.format(spot=spot), json.dumps(msg), qos=1)

    state = {s: {"occ": None, "ctr": 0, "batt": 3600, "last_hb": -1e9,
                 "offset": i * 23} for i, s in enumerate(args.spots)}
    t0 = time.monotonic()
    print(f"simulating {args.spots} -> {args.http or f'{args.host}:{args.port}'}  (Ctrl+C to stop)")
    try:
        while True:
            t = (time.monotonic() - t0) * speed
            if args.duration and t / speed > args.duration:
                break
            for spot, st in state.items():
                if args.silent_after and spot == args.spots[0] and t / speed > args.silent_after:
                    continue
                phase = (t + st["offset"]) % CYCLE_S
                occ = OCC_FROM <= phase < OCC_TO
                kind = None
                if occ != st["occ"]:
                    kind = "change"
                elif (t - st["last_hb"]) >= HEARTBEAT_S:
                    kind = "heartbeat"
                if kind:
                    st["occ"] = occ
                    st["last_hb"] = t
                    msg = make_msg(spot, occ, st["batt"], st["ctr"], kind == "heartbeat")
                    send(spot, msg)
                    print(f"{t / speed:7.1f}s  {json.dumps(msg)}")
                    st["ctr"] += 1
                    if kind == "heartbeat":
                        st["batt"] -= 1
            time.sleep(0.2 / speed)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
