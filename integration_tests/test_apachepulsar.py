"""Run integration tests for Apache Pulsar backend.

Verify basic functionality.
"""

# local imports
from MQClient.backends import apachepulsar

from .common_queue_tests import PubSubQueue, queue_name  # noqa: F401 # pylint: disable=W0611


class TestPulsarQueue(PubSubQueue):
    """Run PubSubQueue integration tests with Pulsar backend."""

    backend = apachepulsar.Backend()
