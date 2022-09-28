"""Run integration tests for RabbitMQ broker_client."""

import logging

from mqclient import broker_client_manager

from ..abstract_broker_client_tests import (
    integrate_broker_client_interface,
    integrate_queue,
)
from ..abstract_broker_client_tests.utils import (  # pytest.fixture # noqa: F401 # pylint: disable=W0611
    queue_name,
)

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("flake8").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)


class TestRabbitMQQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with RabbitMQ broker_client."""

    broker_client = "rabbitmq"


class TestRabbitMQBrokerClient(
    integrate_broker_client_interface.PubSubBrokerClientInterface
):
    """Run PubSubBrokerClientInterface integration tests with RabbitMQ broker_client."""

    broker_client = broker_client_manager.get_broker_client("rabbitmq")
