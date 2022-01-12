"""Unit test Queue class."""

# pylint:disable=invalid-name,protected-access

from functools import partial
from typing import Any, AsyncGenerator, List
from unittest.mock import AsyncMock, sentinel

import pytest

# local imports
from mqclient.backend_interface import Backend, Message
from mqclient.queue import Queue


def test_init() -> None:
    """Test constructor."""
    mock_backend = Backend()
    q = Queue(mock_backend)
    assert q._backend == mock_backend

    q = Queue(Backend(), name="nnn", address="aaa", prefetch=999)
    assert q._name == "nnn"
    assert q._address == "aaa"
    assert q._prefetch == 999


@pytest.mark.asyncio  # type: ignore[misc]
async def test_pub() -> None:
    """Test pub."""
    mock_backend = AsyncMock()
    q = Queue(mock_backend)
    assert (await q.raw_pub_queue) == mock_backend.create_pub_queue.return_value


@pytest.mark.asyncio  # type: ignore[misc]
async def test_send() -> None:
    """Test send."""
    mock_backend = AsyncMock()

    q = Queue(mock_backend)

    data = {"a": 1234}
    await q.send(data)
    mock_backend.create_pub_queue.return_value.send_message.assert_awaited()

    # send() adds a unique header, so we need to look at only the data
    msg = Message(
        id(sentinel.ID),
        mock_backend.create_pub_queue.return_value.send_message.call_args.args[0],
    )
    assert msg.data == data


@pytest.mark.asyncio  # type: ignore[misc]
async def test_recv() -> None:
    """Test recv."""

    # pylint:disable=unused-argument
    async def gen(*args: Any, **kwargs: Any) -> AsyncGenerator[Message, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize(d))

    mock_backend = AsyncMock()

    q = Queue(mock_backend)

    data = ["a", {"b": 100}, ["foo", "bar"]]
    # q.raw_sub_queue.message_generator.side_effect = partial(gen, data)  # type: ignore
    mock_backend.create_sub_queue.return_value.message_generator = gen

    async with q.recv() as recv_gen:
        recv_data = [d async for d in recv_gen]
        assert data == recv_data


@pytest.mark.asyncio  # type: ignore[misc]
async def test_recv_one() -> None:
    """Test recv_one."""
    mock_backend = AsyncMock()

    q = Queue(mock_backend)

    data = {"b": 100}
    msg = Message(0, Message.serialize(data))
    mock_backend.create_sub_queue.return_value.get_message.return_value = msg

    async with q.recv_one() as d:
        recv_data = d

    assert data == recv_data
    mock_backend.create_sub_queue.return_value.ack_message.assert_called_with(msg)
