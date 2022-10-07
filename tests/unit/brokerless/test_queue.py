"""Unit test Queue class."""

# pylint:disable=invalid-name,protected-access

from typing import Any, AsyncGenerator
from unittest.mock import call, patch, sentinel

import pytest
from mqclient.broker_client_interface import AckException, Message, NackException
from mqclient.queue import EmptyQueueException, Queue

try:
    from unittest.mock import AsyncMock
except ImportError:
    from mock import AsyncMock  # use backport


def test_init() -> None:
    """Test constructor."""
    q = Queue("pulsar")
    assert q._broker_client.NAME == "pulsar"

    q = Queue("pulsar", name="nnn", address="aaa", prefetch=999)
    assert q._name == "nnn"
    assert q._address == "aaa"
    assert q._prefetch == 999


@pytest.mark.asyncio
async def test_send() -> None:
    """Test send."""
    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = {"a": 1234}
    async with q.open_pub() as p:
        await p.send(data)
    mock_broker_client.create_pub_queue.return_value.send_message.assert_awaited()
    mock_broker_client.create_pub_queue.return_value.close.assert_called()

    # send() adds a unique header, so we need to look at only the data
    msg = Message(
        id(sentinel.ID),
        mock_broker_client.create_pub_queue.return_value.send_message.call_args.args[0],
    )
    assert msg.data == data


@pytest.mark.asyncio
async def test_open_sub() -> None:
    """Test recv."""

    # pylint:disable=unused-argument
    async def gen(*args: Any, **kwargs: Any) -> AsyncGenerator[Message, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize(d))

    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = ["a", {"b": 100}, ["foo", "bar"]]
    mock_broker_client.create_sub_queue.return_value.message_generator = gen

    async with q.open_sub() as stream:
        recv_data = [d async for d in stream]
        assert data == recv_data
        stream._sub.ack_message.assert_has_calls(
            [call(Message(i, Message.serialize(d))) for i, d in enumerate(recv_data)]
        )

    mock_broker_client.create_sub_queue.return_value.close.assert_called()


@pytest.mark.asyncio
async def test_open_sub_one() -> None:
    """Test open_sub_one."""
    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = {"b": 100}
    msg = Message(0, Message.serialize(data))
    mock_broker_client.create_sub_queue.return_value.get_message.return_value = msg

    async with q.open_sub_one() as d:
        recv_data = d

    assert data == recv_data
    mock_broker_client.create_sub_queue.return_value.ack_message.assert_called_with(msg)
    mock_broker_client.create_sub_queue.return_value.close.assert_called()


@pytest.mark.asyncio
async def test_open_sub_one__no_msg() -> None:
    """Test open_sub_one with an empty queue."""
    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    mock_broker_client.create_sub_queue.return_value.get_message.return_value = None

    with pytest.raises(EmptyQueueException):
        async with q.open_sub_one() as _:
            assert 0  # we should never get here

    mock_broker_client.create_sub_queue.return_value.ack_message.assert_not_called()
    mock_broker_client.create_sub_queue.return_value.reject_message.assert_not_called()
    mock_broker_client.create_sub_queue.return_value.close.assert_called()


@pytest.mark.asyncio
async def test_safe_ack() -> None:
    """Test _safe_ack()."""
    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = {"b": 100}

    # okay/normal
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    assert msg._ack_status == Message.AckStatus.NONE
    await q._safe_ack(mock_sub, msg)
    mock_sub.ack_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.ACKED

    # okay but pointless
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    msg._ack_status = Message.AckStatus.ACKED
    assert msg._ack_status == Message.AckStatus.ACKED
    await q._safe_ack(mock_sub, msg)
    mock_sub.ack_message.assert_not_called()
    assert msg._ack_status == Message.AckStatus.ACKED

    # not okay
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    msg._ack_status = Message.AckStatus.NACKED
    assert msg._ack_status == Message.AckStatus.NACKED
    with pytest.raises(AckException):
        await q._safe_ack(mock_sub, msg)
    mock_sub.ack_message.assert_not_called()
    assert msg._ack_status == Message.AckStatus.NACKED


@pytest.mark.asyncio
async def test_safe_nack() -> None:
    """Test _safe_nack()."""
    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = {"b": 100}

    # okay/normal
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    assert msg._ack_status == Message.AckStatus.NONE
    await q._safe_nack(mock_sub, msg)
    mock_sub.reject_message.assert_called_with(msg)
    assert msg._ack_status == Message.AckStatus.NACKED

    # not okay
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    msg._ack_status = Message.AckStatus.ACKED
    assert msg._ack_status == Message.AckStatus.ACKED
    with pytest.raises(NackException):
        await q._safe_nack(mock_sub, msg)
    mock_sub.reject_message.assert_not_called()
    assert msg._ack_status == Message.AckStatus.ACKED

    # okay but pointless
    mock_sub = AsyncMock()
    msg = Message(0, Message.serialize(data))
    msg._ack_status = Message.AckStatus.NACKED
    assert msg._ack_status == Message.AckStatus.NACKED
    await q._safe_nack(mock_sub, msg)
    mock_sub.reject_message.assert_not_called()
    assert msg._ack_status == Message.AckStatus.NACKED


@pytest.mark.asyncio
async def test_nack_current() -> None:
    """Test recv with nack_current()."""

    # pylint:disable=unused-argument
    async def gen(*args: Any, **kwargs: Any) -> AsyncGenerator[Message, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize(d))

    mock_broker_client = AsyncMock()
    with patch(
        "mqclient.broker_client_manager.get_broker_client"
    ) as mock_get_broker_client:
        mock_get_broker_client.return_value = mock_broker_client
        q = Queue("mock")

    data = ["a", {"b": 100}, ["foo", "bar"]]
    msgs = [Message(i, Message.serialize(d)) for i, d in enumerate(data)]
    mock_broker_client.create_sub_queue.return_value.message_generator = gen

    async with q.open_sub() as stream:
        i = 0
        # manual nacking won't actually place the message for redelivery b/c of mocking
        async for _ in stream:
            if i == 0:  # nack it
                await stream.nack_current()
                stream._sub.reject_message.assert_has_calls([call(msgs[0])])
            elif i == 1:  # DON'T nack it
                stream._sub.ack_message.assert_not_called()  # from i=0
            elif i == 2:  # nack it
                stream._sub.reject_message.assert_has_calls([call(msgs[0])])
                stream._sub.ack_message.assert_has_calls([call(msgs[1])])
                await stream.nack_current()
                stream._sub.reject_message.assert_has_calls(
                    [call(msgs[0]), call(msgs[2])]
                )
            else:
                assert 0
            i += 1
        stream._sub.ack_message.assert_has_calls([call(msgs[1])])
        stream._sub.reject_message.assert_has_calls([call(msgs[0]), call(msgs[2])])
