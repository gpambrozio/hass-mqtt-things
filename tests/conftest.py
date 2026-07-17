import asyncio
import socket
import threading

import pytest

from amqtt.broker import Broker


@pytest.fixture
def broker_port():
    """Run a real MQTT broker on a free local port for the duration of a test."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    config = {
        "listeners": {
            "default": {"type": "tcp", "bind": f"127.0.0.1:{port}"},
        },
        "auth": {"allow-anonymous": True},
        "topic-check": {"enabled": False},
        "sys_interval": 0,
    }

    loop = asyncio.new_event_loop()
    broker_holder = {}
    started = threading.Event()

    def run():
        asyncio.set_event_loop(loop)

        async def start():
            broker_holder["broker"] = Broker(config)
            await broker_holder["broker"].start()

        loop.run_until_complete(start())
        started.set()
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    if not started.wait(10):
        raise RuntimeError("MQTT test broker failed to start")

    yield port

    asyncio.run_coroutine_threadsafe(broker_holder["broker"].shutdown(), loop).result(10)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(10)
