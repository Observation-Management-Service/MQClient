"""
Run integration tests for Apache Pulsar backend.

Verify basic functionality.
"""

import common_tests
from MQClient import backends


def test_queue():
    common_tests.test_queue(backends.pulsar)
