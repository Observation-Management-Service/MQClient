"""Manage the different backends."""

from .backend_interface import Backend


def get_backend(backend_name: str) -> Backend:
    """Get the `Backend` instance per the given name."""

    # Pulsar
    if backend_name.lower() == "pulsar":
        from .backends import apachepulsar

        return apachepulsar.Backend

    # GCP
    elif backend_name.lower() == "gcp":
        from .backends import gcp

        return gcp.Backend

    # NATS
    elif backend_name.lower() == "nats":
        from .backends import nats

        return nats.Backend

    # RabbitMQ
    elif backend_name.lower() == "rabbitmq":
        from .backends import rabbitmq

        return rabbitmq.Backend

    # Error
    else:
        raise RuntimeError(f"Unknown backend: {backend_name}")
