"""Run integration tests for RabbitMQ backend."""

# local imports
from MQClient.backends import rabbitmq

from .common_backend_interface_tests import PubSubBackendInterface
from .common_queue_tests import PubSubQueue
from .utils import queue_name  # pytest.fixture # noqa: F401 # pylint: disable=W0611


class TestRabbitMQQueue(PubSubQueue):
    """Run PubSubQueue integration tests with RabbitMQ backend."""

    backend = rabbitmq.Backend()


class TestRabbitMQBackend(PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with RabbitMQ backend."""

    backend = rabbitmq.Backend()
