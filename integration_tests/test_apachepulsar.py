"""Run integration tests for Apache Pulsar backend.

Verify basic functionality.
"""

# local imports
from MQClient.backends import apachepulsar

from .common_tests import PubSub, queue_name  # noqa: F401 # pylint: disable=W0611


class TestPulsar(PubSub):
    """Run PubSub integration tests with Pulsar backend."""

    backend = apachepulsar.PulsarBackend()
