"""Run integration tests for GCP backend."""

import logging

from mqclient import backend_manager

from ..abstract_backend_tests import integrate_backend_interface, integrate_queue
from ..abstract_backend_tests.utils import (  # pytest.fixture # noqa: F401 # pylint: disable=W0611
    queue_name,
)

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("flake8").setLevel(logging.WARNING)


class TestGCPQueue(integrate_queue.PubSubQueue):
    """Run PubSubQueue integration tests with GCP backend."""

    backend = "gcp"


class TestGCPBackend(integrate_backend_interface.PubSubBackendInterface):
    """Run PubSubBackendInterface integration tests with GCP backend."""

    backend = backend_manager.get_backend("gcp")
