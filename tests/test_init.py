"""Unit test imports."""

import importlib


def test_import() -> None:
    """Test module imports."""
    m = importlib.import_module("MQClient")
    assert hasattr(m, "Queue")
