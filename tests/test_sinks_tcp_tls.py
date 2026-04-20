import socket
import threading
import time

import pytest

from sinks.tcp_sink import TcpSink, _truncate_if_needed


@pytest.fixture
def tcp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(5)
    port = sock.getsockname()[1]
    received = []
    stop = threading.Event()

    def _loop():
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with conn:
                buf = b""
                conn.settimeout(0.3)
                while True:
                    try:
                        chunk = conn.recv(4096)
                    except socket.timeout:
                        break
                    if not chunk:
                        break
                    buf += chunk
                received.append(buf)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    yield ("127.0.0.1", port, received)
    stop.set()
    sock.close()
    t.join(timeout=1)


def test_tcp_send_adds_newline_framing(tcp_listener):
    host, port, received = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=0,
                   retry_backoff_sec=[])
    assert sink.send(b"event1") is True
    sink.close()
    time.sleep(0.3)
    assert received and received[0] == b"event1\n"


def test_tcp_reconnect_on_failure(tcp_listener):
    host, port, received = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=2,
                   retry_backoff_sec=[0.01, 0.01])
    assert sink.send(b"first") is True
    sink.close()
    sink2 = TcpSink(host=host, port=port, max_retries=2,
                    retry_backoff_sec=[0.01, 0.01])
    assert sink2.send(b"second") is True
    sink2.close()


def test_tcp_connect_failure_returns_false():
    sink = TcpSink(host="127.0.0.1", port=1, max_retries=1,
                   retry_backoff_sec=[0.01], timeout_sec=1)
    assert sink.send(b"x") is False
    sink.close()


def test_tcp_truncates_over_8192():
    big = b"A" * 10000
    out, warned = _truncate_if_needed(big)
    assert len(out) == 8192
    assert warned
