"""Utility data and functions."""

import logging
from typing import Any, List, Optional

import pytest
from mqclient.queue import Queue


def is_inst_name(obj: Any, name: str) -> bool:
    """Return the object's name, fully qualified with its module's name."""
    obj_name = f"{obj.__class__.__module__}.{obj.__class__.__name__}"

    return obj_name == name or obj_name.endswith("." + name)


@pytest.fixture
def queue_name() -> str:
    """Get random queue name.

    Obeys the valid naming scheme for GCP (other broker_clients are less picky).
    (See https://cloud.google.com/resource-manager/reference/rest/v1/projects#resource:-project)
    """
    name = Queue.make_name()
    logging.info(f"NAME :: {name}")
    return name


# Note: don't put in duplicates
DATA_LIST = [
    {"abcdefghijklmnop": ["foo", "bar", 3, 4]},
    111,
    "two",
    [1, 2, 3, 4],
    False,
    None,
]


def _log_recv(data: Any) -> Any:
    _log_data("RECV", data)
    return data


def _log_recv_multiple(data: List[Any]) -> List[Any]:
    _log_data("RECV", data, is_list=True)
    return data


def _log_send(data: Any) -> None:
    _log_data("SEND", data)


def _log_data(_type: str, data: Any, is_list: bool = False) -> None:
    if (_type == "RECV") and is_list and isinstance(data, list):
        logging.info(f"{_type} - {len(data)} :: {data}")
    else:
        logging.info(f"{_type} :: {data}")


def all_were_received(recvd: List[Any], expected: Optional[List[Any]] = None) -> bool:
    """Return True if `recvd` list is set equal to `expected`.

    If `expected` is None, use DATA_LIST.
    """
    if expected is None:  # don't override `[]`
        expected = DATA_LIST

    def log_false() -> bool:
        logging.critical(f"received ({len(recvd)}): {recvd}")
        logging.critical(
            f"expected ({len(expected) if expected is not None else 'none'}): {expected}"
        )
        return False

    # can't do set() b/c objects aren't guaranteed to be hashable

    for thing in expected:
        if thing not in recvd:
            return log_false()

    for thing in recvd:
        if thing not in expected:
            return log_false()

    if len(recvd) != len(expected):
        return log_false()

    return True
