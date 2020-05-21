"""
Run integration tests for RabbitMQ backend.

Verify basic functionality.
"""

from MQClient.backends import rabbitmq
from .common_tests import PubSub, queue_name  # noqa: F401

class PulsarTests(PubSub):
    backend = rabbitmq
