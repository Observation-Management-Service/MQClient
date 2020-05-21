"""
Run integration tests for Apache Pulsar backend.

Verify basic functionality.
"""

from MQClient.backends import pulsar
from .common_tests import PubSub, queue_name

class TestPulsar(PubSub):
    backend = pulsar
