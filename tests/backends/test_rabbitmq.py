"""Unit Tests for RabbitMQ/Pika Backend."""

import unittest
from typing import Any, List
from unittest.mock import MagicMock

import pytest  # type: ignore

# local imports
from MQClient.backends import rabbitmq

from .common_unit_tests import BackendUnitTest


class TestUnitRabbitMQ(BackendUnitTest):
    """Unit test suite interface for RabbitMQ backend."""

    backend = rabbitmq.Backend()
    con_patch = 'pika.BlockingConnection'

    @staticmethod
    def _get_mock_nack(mock_con: Any) -> Any:
        """Return mock 'nack' function call."""
        return mock_con.return_value.channel.return_value.basic_nack

    @staticmethod
    def _get_mock_ack(mock_con: Any) -> Any:
        """Return mock 'ack' function call."""
        return mock_con.return_value.channel.return_value.basic_ack

    @staticmethod
    def _get_mock_close(mock_con: Any) -> Any:
        """Return mock 'close' function call."""
        return mock_con.return_value.channel.return_value.cancel

    @staticmethod
    def _enqueue_mock_messages(mock_con: Any, data: List[bytes], ids: List[int],
                               append_none: bool = True) -> None:
        """Place messages on the mock queue."""
        if len(data) != len(ids):
            raise AttributeError("`data` and `ids` must have the same length.")
        messages = [(MagicMock(delivery_tag=i), None, d) for d, i in zip(data, ids)]
        if append_none:
            messages += [(None, None, None)]  # type: ignore
        mock_con.return_value.channel.return_value.consume.return_value = messages

    def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        assert q.queue == queue_name
        mock_con.return_value.channel.assert_called()

    def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        q = self.backend.create_sub_queue("localhost", queue_name, prefetch=213)
        assert q.queue == queue_name
        assert q.prefetch == 213
        mock_con.return_value.channel.assert_called()

    def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        q.send_message(b"foo, bar, baz")
        mock_con.return_value.channel.return_value.basic_publish.assert_called_with(
            exchange='',
            routing_key=queue_name,
            body=b'foo, bar, baz',
        )

    def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
        mock_con.return_value.channel.return_value.basic_get.return_value = fake_message
        m = q.get_message()
        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'

    def test_message_generator_upstream_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pika-package code).
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        err_msg = (unittest.mock.ANY, None, b'foo, bar')
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = list(q.message_generator())
        self._get_mock_close(mock_con).assert_called()

        # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
        err_msg = (unittest.mock.ANY, None, b'foo, bar')
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = list(q.message_generator(propagate_error=False))
        self._get_mock_close(mock_con).assert_called()
