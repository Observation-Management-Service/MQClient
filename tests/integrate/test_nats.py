"""Run integration tests for NATS broker_client."""

import asyncio
import logging

import pytest
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


@pytest.fixture(scope="module")
def event_loop():  # type: ignore[no-untyped-def]
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class TestNATSQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with NATS broker_client."""

    broker_client = "nats"


class TestNATSBrokerClient(
    integrate_broker_client_interface.PubSubBrokerClientInterface
):
    """Run PubSubBrokerClientInterface integration tests with NATS broker_client."""

    broker_client = broker_client_manager.get_broker_client("nats")
