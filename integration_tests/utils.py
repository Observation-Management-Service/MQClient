"""Utility data and functions."""

import logging
import uuid
from typing import Any

import pytest


@pytest.fixture
def queue_name() -> str:
    """Get random queue name.

    Obeys the valid naming scheme for GCP (other backends are less picky).
    (See https://cloud.google.com/resource-manager/reference/rest/v1/projects#resource:-project)
    """
    name = "a" + (uuid.uuid4().hex)[:20]
    logging.info(f"NAME :: {name}")
    return name


# Note: don't put in duplicates
DATA_LIST = [
    {"abcdefghijklmnop": ["foo", "bar", 3, 4]},
    1,
    "2",
    [1, 2, 3, 4],
    False,
    None,
]


def _log_recv(data: Any) -> None:
    _log_data("RECV", data)


def _log_recv_multiple(data: Any) -> None:
    _log_data("RECV", data, is_list=True)


def _log_send(data: Any) -> None:
    _log_data("SEND", data)


def _log_data(_type: str, data: Any, is_list: bool = False) -> None:
    if (_type == "RECV") and is_list and isinstance(data, list):
        logging.info(f"{_type} - {len(data)} :: {data}")
    else:
        logging.info(f"{_type} :: {data}")
