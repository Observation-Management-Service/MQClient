"""Run integration tests for GCP broker_client."""

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


class TestGCPQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with GCP broker_client."""

    broker_client = "gcp"


class TestGCPBrokerClient(
    integrate_broker_client_interface.PubSubBrokerClientInterface
):
    """Run PubSubBrokerClientInterface integration tests with GCP broker_client."""

    broker_client = broker_client_manager.get_broker_client("gcp")
