import socket
import threading
import time

import pytest

from sinks.udp_sink import UdpSink


@pytest.fixture
def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    received = []

    def _loop():
        while True:
            try:
                data, _ = sock.recvfrom(65535)
                if data == b"__STOP__":
                    break
                received.append(data)
            except OSError:
                break

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    yield ("127.0.0.1", port, received)
    try:
        stop = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        stop.sendto(b"__STOP__", ("127.0.0.1", port))
        stop.close()
    finally:
        sock.close()


def test_udp_send_delivers_payload(udp_listener):
    host, port, received = udp_listener
    sink = UdpSink(host=host, port=port)
    assert sink.send(b"hello syslog") is True
    time.sleep(0.1)
    assert b"hello syslog" in received
    sink.close()


def test_udp_truncates_over_1024_bytes(udp_listener):
    host, port, received = udp_listener
    sink = UdpSink(host=host, port=port)
    huge = b"A" * 2000
    sink.send(huge)
    time.sleep(0.1)
    assert len(received[-1]) == 1024
    sink.close()
