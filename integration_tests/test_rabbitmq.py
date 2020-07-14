"""Run integration tests for RabbitMQ backend.

Verify basic functionality.
"""

# local imports
from MQClient.backends import rabbitmq

from .common_tests import PubSub, queue_name  # noqa: F401 # pylint: disable=W0611


class TestRabbitMQ(PubSub):
    """Run PubSub integration tests with RabbitMQ backend."""

    backend = rabbitmq.RabbitMQBackend()
