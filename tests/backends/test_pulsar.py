from unittest.mock import MagicMock

import pulsar as pulsar_client
import pytest  # type: ignore
from MQClient.backends import pulsar


@pytest.fixture
def mock_pulsar(mocker):
    return mocker.patch('pulsar.Client')


def test_create_pub_queue(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test creating pub queue."""
    q = pulsar.create_pub_queue("localhost", "test")
    assert q.queue == "test"
    mock_pulsar.return_value.create_producer.assert_called()


def test_create_sub_queue(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test creating sub queue."""
    q = pulsar.create_sub_queue("localhost", "test", prefetch=213)
    assert q.queue == "test"
    assert q.prefetch == 213
    mock_pulsar.return_value.subscribe.assert_called()


def test_send_message(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test sending message."""
    q = pulsar.create_pub_queue("localhost", "test")
    pulsar.send_message(q, b"foo, bar, baz")
    mock_pulsar.return_value.create_producer.return_value.send.assert_called_with(b'foo, bar, baz')


def test_get_message(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test getting message."""
    q = pulsar.create_sub_queue("localhost", "test")
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.data.return_value = b'foo, bar'
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.message_id.return_value = 12
    m = pulsar.get_message(q)
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'


def test_ack_message(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test acking message."""
    q = pulsar.create_sub_queue("localhost", "test")
    pulsar.ack_message(q, 12)
    mock_pulsar.return_value.subscribe.return_value.acknowledge.assert_called_with(12)


def test_reject_message(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test rejecting message."""
    q = pulsar.create_sub_queue("localhost", "test")
    pulsar.reject_message(q, 12)
    mock_pulsar.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(12)


def test_consume(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test message generator."""
    q = pulsar.create_sub_queue("localhost", "test")
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar', b'baz']
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12, 20]
    m = None
    for i, x in enumerate(pulsar.message_generator(q)):
        if i > 0:
            break
        m = x
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pulsar.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_pulsar.return_value.close.assert_called()


def test_consume2(mock_pulsar) -> None:  # pylint: disable=W0621
    """Test message generator."""
    q = pulsar.create_sub_queue("localhost", "test")
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar', None]
    mock_pulsar.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12, None]
    m = None
    for i, x in enumerate(pulsar.message_generator(q)):
        assert i < 1
        m = x
    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'
    mock_pulsar.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_pulsar.return_value.close.assert_called()
