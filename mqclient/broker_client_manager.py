"""Manage the different broker_clients."""

from .broker_client_interface import BrokerClient

# Import all the broker clients at package import, so any bindings can be built/compiled
# fmt: off
_INSTALLED_BROKERS = {}
# Pulsar
try:
    from .broker_clients import apachepulsar
    _INSTALLED_BROKERS["pulsar"] = apachepulsar
except (ModuleNotFoundError, ImportError):
    _INSTALLED_BROKERS["pulsar"] = None
# GCP
try:
    from .broker_clients import gcp
    _INSTALLED_BROKERS["gcp"] = gcp
except (ModuleNotFoundError, ImportError):
    _INSTALLED_BROKERS["gcp"] = None
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
        return _INSTALLED_BROKERS[broker_client_name].BrokerClient()
    except KeyError:
        raise RuntimeError(f"Unknown broker client: {broker_client_name}")
    except AttributeError:
        raise RuntimeError(
            f"Install 'mqclient[{broker_client_name.lower()}]' "
            f"if you want to use the '{broker_client_name}' broker client"
        )
