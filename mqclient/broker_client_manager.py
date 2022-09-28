"""Manage the different broker_clients."""

from .broker_client_interface import BrokerClient


def get_broker_client(broker_client_name: str) -> BrokerClient:
    """Get the `BrokerClient` instance per the given name."""

    try:
        # Pulsar
        if broker_client_name.lower() == "pulsar":
            from .broker_clients import apachepulsar

            return apachepulsar.BrokerClient()

        # GCP
        elif broker_client_name.lower() == "gcp":
            from .broker_clients import gcp

            return gcp.BrokerClient()

        # NATS
        elif broker_client_name.lower() == "nats":
            from .broker_clients import nats

            return nats.BrokerClient()

        # RabbitMQ
        elif broker_client_name.lower() == "rabbitmq":
            from .broker_clients import rabbitmq

            return rabbitmq.BrokerClient()

        # Error
        else:
            raise RuntimeError(f"Unknown broker_client: {broker_client_name}")

    except ModuleNotFoundError as e:
        raise RuntimeError(
            f"Install 'mqclient[{broker_client_name.lower()}]' "
            f"if you want to use the '{broker_client_name}' broker_client"
        ) from e
