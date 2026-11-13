"""MQTT subscriber: receives node messages and hands them to the Processor."""
import logging
import uuid

import paho.mqtt.client as mqtt

log = logging.getLogger("parking.mqtt")


class MqttIngest:
    def __init__(self, processor, host: str, port: int, topic: str):
        self.processor = processor
        self.host, self.port, self.topic = host, port, topic
        self.connected = False
        self.received = 0
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=f"parking-backend-{uuid.uuid4().hex[:8]}")
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

    def start(self) -> None:
        log.info("connecting to MQTT %s:%s, topic %s", self.host, self.port, self.topic)
        self.client.connect_async(self.host, self.port, keepalive=30)
        self.client.loop_start()          # background thread, reconnects automatically

    def stop(self) -> None:
        self.client.loop_stop()
        try:
            self.client.disconnect()
        except Exception:
            pass

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.warning("MQTT connect failed: %s", reason_code)
            return
        self.connected = True
        client.subscribe(self.topic, qos=1)
        log.info("MQTT connected, subscribed to %s", self.topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected = False
        log.warning("MQTT disconnected (%s); will retry", reason_code)

    def _on_message(self, client, userdata, msg):
        self.received += 1
        result = self.processor.handle(msg.payload, msg.topic)
        if not result.get("accepted"):
            log.warning("rejected message on %s: %s", msg.topic, result.get("reason"))
        elif result.get("duplicate"):
            log.info("duplicate ignored on %s", msg.topic)
        else:
            log.info("validated %s -> %s%s%s", msg.payload.decode("utf-8", "replace"),
                     result["space_id"],
                     " (status changed)" if result.get("status_changed") else "",
                     f" ({result['lost_messages']} lost)" if result.get("lost_messages") else "")
