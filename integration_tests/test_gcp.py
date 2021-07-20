"""Run integration tests for GCP backend."""

# local imports
from MQClient.backends import gcp

from .common_backend_interface_tests import PubSubBackendInterface
from .common_queue_tests import PubSubQueue
from .utils import queue_name  # pytest.fixture # noqa: F401 # pylint: disable=W0611


class TestGCPQueue(PubSubQueue):
    """Run PubSubQueue integration tests with GCP backend."""

    backend = gcp.Backend()


class TestGCPBackend(PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with GCP backend."""

    backend = gcp.Backend()
