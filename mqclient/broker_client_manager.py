"""Manage the different broker_clients."""

from types import ModuleType
from typing import Dict, Optional

from .broker_client_interface import BrokerClient, MQClientException

# Import all the broker clients at package import, so any bindings can be built/compiled
# fmt: off
_INSTALLED_BROKERS: Dict[str, Optional[ModuleType]] = {}
# Pulsar
try:
    from .broker_clients import apachepulsar
    _INSTALLED_BROKERS["pulsar"] = apachepulsar
except (ModuleNotFoundError, ImportError):
    _INSTALLED_BROKERS["pulsar"] = None
# NATS
try:
    from .broker_clients import nats
    _INSTALLED_BROKERS["nats"] = nats
except (ModuleNotFoundError, ImportError):
    _INSTALLED_BROKERS["nats"] = None
# RabbitMQ
try:
    from .broker_clients import rabbitmq
    _INSTALLED_BROKERS["rabbitmq"] = rabbitmq
except (ModuleNotFoundError, ImportError):
    _INSTALLED_BROKERS["rabbitmq"] = None
# fmt: on


def get_broker_client(broker_client_name: str) -> BrokerClient:
    """Get the `BrokerClient` instance per the given name."""
    try:
        module = _INSTALLED_BROKERS[broker_client_name]
    except KeyError:
        raise MQClientException(f"Unknown broker client: {broker_client_name}")

    if not module:
        raise MQClientException(
            f"Install the '{broker_client_name.lower()}' extra "
            f"if you want to use the '{broker_client_name}' broker client"
        )
    return module.BrokerClient()  # type: ignore[no-any-return]
