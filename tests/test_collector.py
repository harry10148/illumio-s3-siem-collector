"""collector.py entry-point behaviors."""
import signal

import pytest


def test_sigterm_handler_raises_systemexit():
    from collector import _install_sigterm_exit
    old = signal.getsignal(signal.SIGTERM)
    try:
        _install_sigterm_exit()
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        with pytest.raises(SystemExit):
            handler(signal.SIGTERM, None)
    finally:
        signal.signal(signal.SIGTERM, old)
