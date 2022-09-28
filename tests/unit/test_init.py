"""Unit test imports."""

import importlib


def test_import() -> None:
    """Test module imports."""
    m = importlib.import_module("mqclient")
    print(dir(m))
    assert hasattr(m, "queue")
    assert hasattr(m, "broker_client_interface")
