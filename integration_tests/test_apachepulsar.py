"""Run integration tests for Apache Pulsar backend."""

# local imports
from MQClient.backends import apachepulsar

from .common_backend_interface_tests import PubSubBackendInterface
from .common_queue_tests import PubSubQueue
from .utils import queue_name  # pytest.fixture # noqa: F401 # pylint: disable=W0611


class TestPulsarQueue(PubSubQueue):
    """Run PubSubQueue integration tests with Pulsar backend."""

    backend = apachepulsar.Backend()


class TestPulsarBackendInterface(PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with Pulsar backend."""

    backend = apachepulsar.Backend()
