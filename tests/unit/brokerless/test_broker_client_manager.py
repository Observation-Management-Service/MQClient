"""Unit test the broker client manager."""

import re

import pytest
from mqclient import broker_client_manager
from mqclient.broker_client_interface import MQClientException


def test_missing_broker_clients() -> None:
    """Test legitimate, but not-installed broker clients."""
    for name in ["pulsar", "rabbitmq", "nats"]:
        with pytest.raises(
            MQClientException,
            match=re.escape(
                f"Install the '{name}' extra if you want to use the '{name}' broker client"
            ),
        ):
            broker_client_manager.get_broker_client(name)


def test_invalid_broker_clients() -> None:
    """Test illegitimate broker clients."""
    for name in ["foo", "bar", "baz"]:
        with pytest.raises(
            MQClientException,
            match=re.escape(f"Unknown broker client: {name}"),
        ):
            broker_client_manager.get_broker_client(name)
