"""Unit Tests for Pulsar Backend."""

# pylint: disable=redefined-outer-name

import logging
from typing import Any, List, Optional

import pytest  # type: ignore

# local imports
from MQClient.backends import apachepulsar

logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture  # type: ignore
def mock_ap(mocker: Any) -> Any:
    """Patch mock_ap."""
    return mocker.patch('pulsar.Client')


def test_create_pub_queue(mock_ap: Any) -> None:
    """Test creating pub queue."""
    q = apachepulsar.create_pub_queue("localhost", "test")
    assert q.topic == "test"
    mock_ap.return_value.create_producer.assert_called()


def test_create_sub_queue(mock_ap: Any) -> None:
    """Test creating sub queue."""
    q = apachepulsar.create_sub_queue("localhost", "test", prefetch=213)
    assert q.topic == "test"
    assert q.prefetch == 213
    mock_ap.return_value.subscribe.assert_called()


def test_send_message(mock_ap: Any) -> None:
    """Test sending message."""
    q = apachepulsar.create_pub_queue("localhost", "test")
    apachepulsar.send_message(q, b"foo, bar, baz")
    mock_ap.return_value.create_producer.return_value.send.assert_called_with(b'foo, bar, baz')


def test_get_message(mock_ap: Any) -> None:
    """Test getting message."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.return_value = b'foo, bar'
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.return_value = 12
    m = apachepulsar.get_message(q)

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'


def test_ack_message(mock_ap: Any) -> None:
    """Test acking message."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    apachepulsar.ack_message(q, 12)
    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)


def test_reject_message(mock_ap: Any) -> None:
    """Test rejecting message."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    apachepulsar.reject_message(q, 12)
    mock_ap.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(12)


def test_message_generator_0(mock_ap: Any) -> None:
    """Test message generator."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    num_msgs = 100

    fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]  # type: List[Optional[bytes]]
    fake_data += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

    fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
    fake_ids += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

    for i, msg in enumerate(apachepulsar.message_generator(q)):
        logging.debug(i)
        if i > 0:  # see if previous msg was acked
            mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((i - 1) * 10)
        assert msg is not None
        assert msg.msg_id == i * 10
        assert msg.data == fake_data[i]
    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((num_msgs - 1) * 10)
    mock_ap.return_value.close.assert_called()


def test_message_generator_1(mock_ap: Any) -> None:
    """Test message generator."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar', b'baz']
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12, 20]

    m = None
    for i, x in enumerate(apachepulsar.message_generator(q)):
        m = x
        if i == 0:
            break

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'

    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_ap.return_value.close.assert_called()


def test_message_generator_2(mock_ap: Any) -> None:
    """Test message generator."""
    q = apachepulsar.create_sub_queue("localhost", "test")
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar',
        None  # signifies end of stream -- not actually a message
    ]
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12,
        None  # signifies end of stream -- not actually a message
    ]

    m = None
    for i, x in enumerate(apachepulsar.message_generator(q)):
        assert i < 1
        m = x

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'

    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_ap.return_value.close.assert_called()


def test_message_generator_upstream_error(mock_ap: Any) -> None:
    """Failure-test message generator.

    Generator should raise Exception originating upstream (a.k.a. from
    pulsar-package code).
    """
    q = apachepulsar.create_sub_queue("localhost", "test")

    mock_ap.return_value.subscribe.return_value.receive.side_effect = Exception()
    with pytest.raises(Exception):
        _ = list(apachepulsar.message_generator(q))
    mock_ap.return_value.close.assert_called()

    # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
    mock_ap.return_value.subscribe.return_value.receive.side_effect = Exception()
    with pytest.raises(Exception):
        _ = list(apachepulsar.message_generator(q, propagate_error=False))
    mock_ap.return_value.close.assert_called()


def test_message_generator_propagate_error(mock_ap: Any) -> None:
    """Failure-test message generator.

    Generator should raise Exception, nack, and close. Unlike in an
    integration test, nacked messages are not put back on the queue.
    """
    pass


def test_message_generator_suppress_error(mock_ap: Any) -> None:
    """Failure-test message generator.

    Generator should not raise Exception. Unlike in an integration test,
    nacked messages are not put back on the queue.
    """
    pass
