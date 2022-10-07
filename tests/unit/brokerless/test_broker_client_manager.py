"""Unit test the broker client manager."""

import pytest
from mqclient import broker_client_manager


def test_missing_broker_clients() -> None:
    """Test legitimate, but not-installed broker clients."""
    for name in ["pulsar", "gcp", "rabbitmq", "nats"]:
        with pytest.raises(
            RuntimeError,
            match=f"Install 'mqclient[{name}]' if you want to use the '{name}' broker client",
        ):
            broker_client_manager.get_broker_client(name)


def test_invalid_broker_clients() -> None:
    """Test illegitimate broker clients."""
    for name in ["foo", "bar", "baz"]:
        with pytest.raises(RuntimeError, match=f"Unknown broker client: {name}"):
            broker_client_manager.get_broker_client(name)
