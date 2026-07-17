import queue
import time

import paho.mqtt.client as mqtt
import pytest

from ham import MqttManager

NODE_ID = "testnode"
AVAILABILITY_TOPIC = f"{NODE_ID}/availability"


@pytest.fixture
def manager(broker_port):
    manager = MqttManager("127.0.0.1", broker_port, node_id=NODE_ID)
    manager.daemon = True
    manager.start()
    deadline = time.monotonic() + 10
    while not manager.client.is_connected():
        if time.monotonic() > deadline:
            raise RuntimeError("MqttManager did not connect to test broker")
        time.sleep(0.05)
    yield manager
    manager.client.disconnect()
    manager.join(10)


@pytest.fixture
def observer(broker_port):
    """A second client that watches the availability topic and can publish to it."""
    messages = queue.Queue()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = lambda c, u, f, rc, p: c.subscribe(AVAILABILITY_TOPIC)
    client.on_message = lambda c, u, msg: messages.put(msg.payload)
    client.connect("127.0.0.1", broker_port)
    client.loop_start()
    yield client, messages
    client.loop_stop()
    client.disconnect()


def wait_for_payload(messages, payload, timeout):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            if messages.get(timeout=remaining) == payload:
                return True
        except queue.Empty:
            return False


def read_retained(broker_port, topic, timeout=5):
    """Connect fresh and return the retained payload on `topic`, or None."""
    result = queue.Queue()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = lambda c, u, f, rc, p: c.subscribe(topic)
    client.on_message = lambda c, u, msg: msg.retain and result.put(msg.payload)
    client.connect("127.0.0.1", broker_port)
    client.loop_start()
    try:
        return result.get(timeout=timeout)
    except queue.Empty:
        return None
    finally:
        client.loop_stop()
        client.disconnect()


def test_republishes_online_when_retained_offline_arrives(broker_port, manager, observer):
    """A hard host reboot can make the broker deliver the previous session's
    last-will (retained "offline") AFTER the new session already published
    "online". The manager must notice and republish "online"."""
    observer_client, messages = observer
    assert wait_for_payload(messages, b"online", timeout=10), \
        "manager never published its initial 'online'"

    # Simulate the stale last-will of the previous (dead) session landing late.
    observer_client.publish(AVAILABILITY_TOPIC, "offline", retain=True)

    assert wait_for_payload(messages, b"online", timeout=5), \
        "manager did not republish 'online' after a stale 'offline' arrived"
    assert read_retained(broker_port, AVAILABILITY_TOPIC) == b"online"
