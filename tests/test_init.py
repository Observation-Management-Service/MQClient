"""Unit test imports."""

import importlib


def test_import() -> None:
    """Test module imports."""
    m = importlib.import_module('MQClient')
    assert hasattr(m, 'Queue')

    m = importlib.import_module('MQClient.backends')
    assert hasattr(m, 'rabbitmq')
    assert hasattr(m, 'apachepulsar')
