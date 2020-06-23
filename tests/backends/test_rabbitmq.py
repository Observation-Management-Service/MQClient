"""Unit Tests for RabbitMQ/Pika Backend."""

# pylint: disable=redefined-outer-name

import logging
from typing import Any
from unittest.mock import MagicMock

import pytest  # type: ignore

# local imports
from MQClient.backends import rabbitmq

logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture  # type: ignore
def mock_pika(mocker: Any) -> Any:
    """Patch mock_pika."""
    return mocker.patch('pika.BlockingConnection')


def test_create_pub_queue(mock_pika: Any) -> None:
    """Test creating pub queue."""
    q = rabbitmq.create_pub_queue("localhost", "test")
    assert q.queue == "test"
    mock_pika.return_value.channel.assert_called()


def test_create_sub_queue(mock_pika: Any) -> None:
    """Test creating sub queue."""
    q = rabbitmq.create_sub_queue("localhost", "test", prefetch=213)
    assert q.queue == "test"
    assert q.prefetch == 213
    mock_pika.return_value.channel.assert_called()


def test_send_message(mock_pika: Any) -> None:
    """Test sending message."""
    q = rabbitmq.create_pub_queue("localhost", "test")
    rabbitmq.send_message(q, b"foo, bar, baz")
    mock_pika.return_value.channel.return_value.basic_publish.assert_called_with(
        exchange='',
        routing_key='test',
        body=b'foo, bar, baz',
    )


def test_get_message(mock_pika: Any) -> None:
    """Test getting message."""
    q = rabbitmq.create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    mock_pika.return_value.channel.return_value.basic_get.return_value = fake_message
    m = rabbitmq.get_message(q)
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'


def test_ack_message(mock_pika: Any) -> None:
    """Test acking message."""
    q = rabbitmq.create_sub_queue("localhost", "test")
    rabbitmq.ack_message(q, 12)
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)


def test_reject_message(mock_pika: Any) -> None:
    """Test rejecting message."""
    q = rabbitmq.create_sub_queue("localhost", "test")
    rabbitmq.reject_message(q, 12)
    mock_pika.return_value.channel.return_value.basic_nack.assert_called_with(12)


def test_message_generator(mock_pika: Any) -> None:
    """Test message generator."""
    q = rabbitmq.create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    fake_message2 = (MagicMock(delivery_tag=20), None, b'baz')
    mock_pika.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
    m = None
    for i, x in enumerate(rabbitmq.message_generator(q)):
        if i > 0:
            break
        m = x
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)
    mock_pika.return_value.channel.return_value.cancel.assert_called()


def test_message_generator_2(mock_pika: Any) -> None:
    """Test message generator."""
    q = rabbitmq.create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    fake_message2 = (None, None, None)
    mock_pika.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
    m = None
    for i, x in enumerate(rabbitmq.message_generator(q)):
        assert i < 1
        m = x
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)
    mock_pika.return_value.channel.return_value.cancel.assert_called()
