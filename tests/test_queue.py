"""Unit test Queue class."""

# pylint:disable=invalid-name,protected-access

from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, call, sentinel

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


@pytest.mark.asyncio
async def test_send() -> None:
    """Test send."""
    mock_backend = AsyncMock()
    q = Queue(mock_backend)

    data = {"a": 1234}
    async with q.sender() as s:
        await s.send(data)
    mock_backend.create_pub_queue.return_value.send_message.assert_awaited()

    # send() adds a unique header, so we need to look at only the data
    msg = Message(
        id(sentinel.ID),
        mock_backend.create_pub_queue.return_value.send_message.call_args.args[0],
    )
    assert msg.data == data


@pytest.mark.asyncio
async def test_recv() -> None:
    """Test recv."""

    # pylint:disable=unused-argument
    async def gen(*args: Any, **kwargs: Any) -> AsyncGenerator[Message, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize(d))

    mock_backend = AsyncMock()
    q = Queue(mock_backend)

    data = ["a", {"b": 100}, ["foo", "bar"]]
    mock_backend.create_sub_queue.return_value.message_generator = gen

    async with q.recv() as recv_gen:
        recv_data = [d async for d in recv_gen]
        assert data == recv_data
        mock_backend.create_sub_queue.return_value.ack_message.assert_has_calls(
            [call(Message(i, Message.serialize(d))) for i, d in enumerate(recv_data)]
        )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_safe_ack() -> None:
    """Test _safe_ack()."""
    mock_backend = AsyncMock()
    q = Queue(mock_backend)

    data = {"b": 100}
    msg = Message(0, Message.serialize(data))

    assert msg._ack_status == Message.AckStatus.NONE
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.ack_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.ACK

    assert msg._ack_status == Message.AckStatus.ACK
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.ack_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.ACK

    assert msg._ack_status == Message.AckStatus.NACK
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.ack_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.NACK


@pytest.mark.asyncio
async def test_safe_nack() -> None:
    """Test _safe_nack()."""
    mock_backend = AsyncMock()
    q = Queue(mock_backend)

    data = {"b": 100}
    msg = Message(0, Message.serialize(data))

    assert msg._ack_status == Message.AckStatus.NONE
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.reject_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.NACK

    assert msg._ack_status == Message.AckStatus.ACK
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.reject_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.ACK

    assert msg._ack_status == Message.AckStatus.NACK
    q._safe_nack(AsyncMock(), msg)
    mock_backend.create_sub_queue.return_value.reject_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.NACK


@pytest.mark.asyncio
async def test_nack_previous() -> None:
    """Test recv with nack_current()."""

    # pylint:disable=unused-argument
    async def gen(*args: Any, **kwargs: Any) -> AsyncGenerator[Message, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize(d))

    mock_backend = AsyncMock()
    q = Queue(mock_backend)

    data = ["a", {"b": 100}, ["foo", "bar"]]
    mock_backend.create_sub_queue.return_value.message_generator = gen

    async with q.recv() as recv_gen:
        i = 0
        # manual nacking won't actually place the message for redelivery b/c of mocking
        async for data in recv_gen:
            mock_backend.create_sub_queue.return_value.ack_message.assert_not_called()
            recv_gen.nack_current()
            mock_backend.create_sub_queue.return_value.reject_message.assert_called_with(
                Message(i, Message.serialize(data))
            )
            i += 1
