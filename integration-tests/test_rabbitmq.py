"""
Run integration tests for RabbitMQ backend.

Verify basic functionality.
"""

import common_tests
from MQClient.backends import rabbitmq as BACKEND


def test_10():
    common_tests.test_10(BACKEND)


def test_11():
    common_tests.test_11(BACKEND)


def test_20():
    common_tests.test_20(BACKEND)


def test_21():
    common_tests.test_21(BACKEND)


def test_30():
    common_tests.test_30(BACKEND)


def test_40():
    common_tests.test_40(BACKEND)


def test_50():
    common_tests.test_50(BACKEND)
