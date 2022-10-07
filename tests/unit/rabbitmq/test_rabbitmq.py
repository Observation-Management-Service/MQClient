"""Unit Tests for RabbitMQ/Pika BrokerClient."""

import unittest
from typing import Any, List
from unittest.mock import MagicMock

import pytest
from mqclient import broker_client_manager
from mqclient.broker_client_interface import Message

from ...abstract_broker_client_tests.unit_tests import BrokerClientUnitTest


class TestUnitRabbitMQ(BrokerClientUnitTest):
    """Unit test suite interface for RabbitMQ broker_client."""

    broker_client = broker_client_manager.get_broker_client("rabbitmq")
    con_patch = "pika.BlockingConnection"

    @staticmethod
    def _get_nack_mock_fn(mock_con: Any) -> Any:
        """Return mock 'nack' function call."""
        return mock_con.return_value.channel.return_value.basic_nack

    @staticmethod
    def _get_ack_mock_fn(mock_con: Any) -> Any:
        """Return mock 'ack' function call."""
        return mock_con.return_value.channel.return_value.basic_ack

    @staticmethod
    def _get_close_mock_fn(mock_con: Any) -> Any:
        """Return mock 'close' function call."""
        return mock_con.return_value.close

    @staticmethod
    async def _enqueue_mock_messages(
        mock_con: Any, data: List[bytes], ids: List[int], append_none: bool = True
    ) -> None:
        """Place messages on the mock queue."""
        if len(data) != len(ids):
            raise AttributeError("`data` and `ids` must have the same length.")
        messages = [(MagicMock(delivery_tag=i), None, d) for d, i in zip(data, ids)]
        if append_none:
            messages += [(None, None, None)]  # type: ignore
        mock_con.return_value.channel.return_value.consume.return_value = messages

    @pytest.mark.asyncio
    async def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        pub = await self.broker_client.create_pub_queue("localhost", queue_name)
        assert pub.queue == queue_name  # type: ignore
        mock_con.return_value.channel.assert_called()

    @pytest.mark.asyncio
    async def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        sub = await self.broker_client.create_sub_queue(
            "localhost", queue_name, prefetch=213
        )
        assert sub.queue == queue_name  # type: ignore
        assert sub.prefetch == 213  # type: ignore
        mock_con.return_value.channel.assert_called()

    @pytest.mark.asyncio
    async def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        pub = await self.broker_client.create_pub_queue("localhost", queue_name)
        await pub.send_message(b"foo, bar, baz")
        mock_con.return_value.channel.return_value.basic_publish.assert_called_with(
            exchange="", routing_key=queue_name, body=b"foo, bar, baz"
        )

    @pytest.mark.asyncio
    async def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        mock_con.return_value.is_closed = False  # HACK - manually set attr

        fake_message = (MagicMock(delivery_tag=12), None, Message.serialize("foo, bar"))
        mock_con.return_value.channel.return_value.basic_get.return_value = fake_message
        m = await sub.get_message()
        assert m is not None
        assert m.msg_id == 12
        assert m.data == "foo, bar"

    @pytest.mark.asyncio
    async def test_message_generator_10_upstream_error(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pika-package code).
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        mock_con.return_value.is_closed = False  # HACK - manually set attr

        err_msg = (unittest.mock.ANY, None, b"foo, bar")
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = [m async for m in sub.message_generator()]
        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

        # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
        err_msg = (unittest.mock.ANY, None, b"foo, bar")
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = [m async for m in sub.message_generator(propagate_error=False)]
        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()
