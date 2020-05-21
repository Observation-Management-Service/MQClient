"""
Run integration tests for RabbitMQ backend.

Verify basic functionality.
"""

from MQClient.backends import rabbitmq
from .common_tests import PubSub, queue_name

class PulsarTests(PubSub):
    backend = rabbitmq
