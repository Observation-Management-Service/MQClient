"""Unit Tests for RabbitMQ/Pika Backend."""

# pylint: disable=redefined-outer-name

import logging
import unittest
from typing import Any, List, Optional, Tuple
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
    q = rabbitmq.Backend().create_pub_queue("localhost", "test")
    assert q.queue == "test"
    mock_pika.return_value.channel.assert_called()


def test_create_sub_queue(mock_pika: Any) -> None:
    """Test creating sub queue."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test", prefetch=213)
    assert q.queue == "test"
    assert q.prefetch == 213
    mock_pika.return_value.channel.assert_called()


def test_send_message(mock_pika: Any) -> None:
    """Test sending message."""
    q = rabbitmq.Backend().create_pub_queue("localhost", "test")
    q.send_message(b"foo, bar, baz")
    mock_pika.return_value.channel.return_value.basic_publish.assert_called_with(
        exchange='',
        routing_key='test',
        body=b'foo, bar, baz',
    )


def test_get_message(mock_pika: Any) -> None:
    """Test getting message."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    mock_pika.return_value.channel.return_value.basic_get.return_value = fake_message
    m = q.get_message()
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'


def test_ack_message(mock_pika: Any) -> None:
    """Test acking message."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    q.ack_message(12)
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)


def test_reject_message(mock_pika: Any) -> None:
    """Test rejecting message."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    q.reject_message(12)
    mock_pika.return_value.channel.return_value.basic_nack.assert_called_with(12)


def test_message_generator_0(mock_pika: Any) -> None:
    """Test message generator."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    num_msgs = 100

    fake_messages = [(MagicMock(delivery_tag=i * 10), None, f'baz-{i}'.encode('utf-8')) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
    fake_messages += [(None, None, None)]  # signifies end of stream -- not actually a message
    mock_pika.return_value.channel.return_value.consume.return_value = fake_messages

    for i, msg in enumerate(q.message_generator()):
        logging.debug(i)
        if i > 0:  # see if previous msg was acked
            mock_pika.return_value.channel.return_value.basic_ack.assert_called_with((i - 1) * 10)
        assert msg is not None
        assert msg.msg_id == i * 10
        assert msg.data == fake_messages[i][2]
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with((num_msgs - 1) * 10)
    mock_pika.return_value.channel.return_value.cancel.assert_called()


def test_message_generator_1(mock_pika: Any) -> None:
    """Test message generator."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    fake_message2 = (MagicMock(delivery_tag=20), None, b'baz')
    mock_pika.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
    m = None
    for i, x in enumerate(q.message_generator()):
        m = x
        if i == 0:
            break

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)
    mock_pika.return_value.channel.return_value.cancel.assert_called()


def test_message_generator_2(mock_pika: Any) -> None:
    """Test message generator."""
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
    fake_message2 = (None, None, None)  # signifies end of stream -- not actually a message
    mock_pika.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
    m = None
    for i, x in enumerate(q.message_generator()):
        assert i < 1
        m = x
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(12)
    mock_pika.return_value.channel.return_value.cancel.assert_called()


def test_message_generator_upstream_error(mock_pika: Any) -> None:
    """Failure-test message generator.

    Generator should raise Exception originating upstream (a.k.a. from
    pika-package code).
    """
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")

    err_msg = (unittest.mock.ANY, None, b'foo, bar')
    mock_pika.return_value.channel.return_value.consume.return_value = [err_msg]
    with pytest.raises(Exception):
        _ = list(q.message_generator())
    mock_pika.return_value.channel.return_value.cancel.assert_called()

    # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
    err_msg = (unittest.mock.ANY, None, b'foo, bar')
    mock_pika.return_value.channel.return_value.consume.return_value = [err_msg]
    with pytest.raises(Exception):
        _ = list(q.message_generator(propagate_error=False))
    mock_pika.return_value.channel.return_value.cancel.assert_called()


def test_message_generator_propagate_error(mock_pika: Any) -> None:
    """Failure-test message generator.

    Generator should raise Exception, nack, and close. Unlike in an
    integration test, nacked messages are not put back on the queue.
    """
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")

    fake_messages = [
        (MagicMock(delivery_tag=0), None, b'baz-0'),
        (MagicMock(delivery_tag=1), None, b'baz-1'),
        (MagicMock(delivery_tag=2), None, b'baz-2')
    ]
    mock_pika.return_value.channel.return_value.consume.return_value = fake_messages

    gen = q.message_generator()  # propagate_error=True
    i = 0
    for msg in gen:
        logging.debug(i)
        assert i < 3
        if i > 0:  # see if previous msg was acked
            mock_pika.return_value.channel.return_value.basic_ack.assert_called_with(i - 1)

        assert msg is not None
        assert msg.msg_id == i
        assert msg.data == fake_messages[i][2]

        if i == 2:
            with pytest.raises(Exception):
                gen.throw(Exception)
            mock_pika.return_value.channel.return_value.basic_nack.assert_called_with(i)
            mock_pika.return_value.channel.return_value.cancel.assert_called()

        i += 1


def test_message_generator_suppress_error(mock_pika: Any) -> None:
    """Failure-test message generator.

    Generator should not raise Exception. Unlike in an integration test,
    nacked messages are not put back on the queue.
    """
    q = rabbitmq.Backend().create_sub_queue("localhost", "test")
    num_msgs = 11
    if num_msgs % 2 == 0:
        raise RuntimeError("`num_msgs` must be odd, so last message is nacked")

    fake_messages = [(MagicMock(delivery_tag=i * 10), None, f'baz-{i}'.encode('utf-8')) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
    fake_messages += [(None, None, None)]  # signifies end of stream -- not actually a message
    mock_pika.return_value.channel.return_value.consume.return_value = fake_messages

    gen = q.message_generator(propagate_error=False)
    i = 0
    # odds are acked and evens are nacked
    for msg in gen:
        logging.debug(i)
        if i > 0:
            if i % 2 == 0:  # see if previous EVEN msg was acked
                mock_pika.return_value.channel.return_value.basic_ack.assert_called_with((i - 1) * 10)
            else:  # see if previous ODD msg was NOT acked
                with pytest.raises(AssertionError):
                    mock_pika.return_value.channel.return_value.basic_ack.assert_called_with((i - 1) * 10)

        assert msg is not None
        assert msg.msg_id == i * 10
        assert msg.data == fake_messages[i][2]

        if i % 2 == 0:
            gen.throw(Exception)
            mock_pika.return_value.channel.return_value.basic_nack.assert_called_with(i * 10)

        i += 1
    mock_pika.return_value.channel.return_value.cancel.assert_called()
