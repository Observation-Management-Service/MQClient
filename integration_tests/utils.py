"""Utility data and functions."""

import uuid
from typing import Any

import pytest  # type: ignore


@pytest.fixture  # type: ignore
def queue_name() -> str:
    """Get random queue name."""
    name = uuid.uuid4().hex
    print(f"NAME :: {name}")
    return name


# Note: don't put in duplicates
DATA_LIST = [{'a': ['foo', 'bar', 3, 4]},
             1,
             '2',
             [1, 2, 3, 4],
             False,
             None
             ]


def _print_recv(data: Any) -> None:
    _print_data("RECV", data)


def _print_recv_multiple(data: Any) -> None:
    _print_data("RECV", data, is_list=True)


def _print_send(data: Any) -> None:
    _print_data("SEND", data)


def _print_data(_type: str, data: Any, is_list: bool = False) -> None:
    if (_type == "RECV") and is_list and isinstance(data, list):
        print(f"{_type} - {len(data)} :: {data}")
    else:
        print(f"{_type} :: {data}")
