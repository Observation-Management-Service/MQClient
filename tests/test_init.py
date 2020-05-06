import importlib


def test_import():
    m = importlib.import_module('MQClient')
    assert hasattr(m, 'Queue')

    m = importlib.import_module('MQClient.backends')
    assert hasattr(m, 'rabbitmq')
    assert hasattr(m, 'pulsar')
