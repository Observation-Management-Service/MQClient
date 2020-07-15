"""Run integration tests for given backend, on backend_interface classes.

Verify basic functionality.
"""

# pylint: disable=redefined-outer-name

import logging
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List

import pytest  # type: ignore

# local imports
from MQClient.backend_interface import Backend

from .utils import DATA_LIST, _print_recv, _print_recv_multiple, _print_send


class PubSubBackendInterface:
    """Integration test suite for Queue objects."""

    backend = None  # type: Backend
