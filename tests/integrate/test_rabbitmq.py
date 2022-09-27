"""Run integration tests for RabbitMQ backend."""

import logging

from mqclient import backend_manager

from ..abstract_backend_tests import integrate_backend_interface, integrate_queue
from ..abstract_backend_tests.utils import (  # pytest.fixture # noqa: F401 # pylint: disable=W0611
    queue_name,
)

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("flake8").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)


class TestRabbitMQQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with RabbitMQ backend."""

    backend = "rabbitmq"


class TestRabbitMQBackend(integrate_backend_interface.PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with RabbitMQ backend."""

    def __init__(self) -> None:
        super().__init__(backend_manager.get_backend("rabbitmq"))
