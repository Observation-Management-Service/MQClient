"""Unit Tests for Pulsar Backend."""

# pylint: disable=redefined-outer-name

import logging
import uuid
from typing import Any, List, Optional

import pytest  # type: ignore

# local imports
from MQClient.backends import apachepulsar

logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture  # type: ignore
def mock_ap(mocker: Any) -> Any:
    """Patch mock_ap."""
    return mocker.patch('pulsar.Client')


@pytest.fixture  # type: ignore
def queue_name() -> str:
    """Get random queue name."""
    name = uuid.uuid4().hex
    logging.info(f"NAME :: {name}")
    return name


def test_create_pub_queue(mock_ap: Any, queue_name: str) -> None:
    """Test creating pub queue."""
    q = apachepulsar.Backend().create_pub_queue("localhost", queue_name)
    assert q.topic == queue_name
    mock_ap.return_value.create_producer.assert_called()


def test_create_sub_queue(mock_ap: Any, queue_name: str) -> None:
    """Test creating sub queue."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name, prefetch=213)
    assert q.topic == queue_name
    assert q.prefetch == 213
    mock_ap.return_value.subscribe.assert_called()


def test_send_message(mock_ap: Any, queue_name: str) -> None:
    """Test sending message."""
    q = apachepulsar.Backend().create_pub_queue("localhost", queue_name)
    q.send_message(b"foo, bar, baz")
    mock_ap.return_value.create_producer.return_value.send.assert_called_with(b'foo, bar, baz')


def test_get_message(mock_ap: Any, queue_name: str) -> None:
    """Test getting message."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.return_value = b'foo, bar'
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.return_value = 12
    m = q.get_message()

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'


def test_ack_message(mock_ap: Any, queue_name: str) -> None:
    """Test acking message."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    q.ack_message(12)
    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)


def test_reject_message(mock_ap: Any, queue_name: str) -> None:
    """Test rejecting message."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    q.reject_message(12)
    mock_ap.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(12)


def test_message_generator_0(mock_ap: Any, queue_name: str) -> None:
    """Test message generator."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    num_msgs = 100

    fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]  # type: List[Optional[bytes]]
    fake_data += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

    fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
    fake_ids += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

    for i, msg in enumerate(q.message_generator()):
        logging.debug(i)
        if i > 0:  # see if previous msg was acked
            mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((i - 1) * 10)
        assert msg is not None
        assert msg.msg_id == i * 10
        assert msg.data == fake_data[i]
    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((num_msgs - 1) * 10)
    mock_ap.return_value.close.assert_called()


def test_message_generator_1(mock_ap: Any, queue_name: str) -> None:
    """Test message generator."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar', b'baz']
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12, 20]

    m = None
    for i, x in enumerate(q.message_generator()):
        m = x
        if i == 0:
            break

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'

    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_ap.return_value.close.assert_called()


def test_message_generator_2(mock_ap: Any, queue_name: str) -> None:
    """Test message generator."""
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
        b'foo, bar',
        None  # signifies end of stream -- not actually a message
    ]
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
        12,
        None  # signifies end of stream -- not actually a message
    ]

    m = None
    for i, x in enumerate(q.message_generator()):
        assert i < 1
        m = x

    assert m is not None
    assert m.msg_id == 12
    assert m.data == b'foo, bar'

    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
    mock_ap.return_value.close.assert_called()


def test_message_generator_upstream_error(mock_ap: Any, queue_name: str) -> None:
    """Failure-test message generator.

    Generator should raise Exception originating upstream (a.k.a. from
    pulsar-package code).
    """
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)

    mock_ap.return_value.subscribe.return_value.receive.side_effect = Exception()
    with pytest.raises(Exception):
        _ = list(q.message_generator())
    mock_ap.return_value.close.assert_called()

    # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
    mock_ap.return_value.subscribe.return_value.receive.side_effect = Exception()
    with pytest.raises(Exception):
        _ = list(q.message_generator(propagate_error=False))
    mock_ap.return_value.close.assert_called()


def test_message_generator_no_auto_ack(mock_ap: Any, queue_name: str) -> None:
    """Test message generator.

    Generator should not ack messages.
    """
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)

    fake_data = [b'baz-0', b'baz-1', b'baz-2']  # type: List[Optional[bytes]]
    fake_data += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data
    fake_ids = [0, 1, 2]  # type: List[Optional[int]]
    fake_ids += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

    gen = q.message_generator(auto_ack=False)
    i = 0
    for msg in gen:
        logging.debug(i)
        if i > 0:  # see if previous msg was acked
            mock_ap.return_value.subscribe.return_value.acknowledge.assert_not_called()

        assert msg is not None
        assert msg.msg_id == i
        assert msg.data == fake_data[i]

        i += 1


def test_message_generator_propagate_error(mock_ap: Any, queue_name: str) -> None:
    """Failure-test message generator.

    Generator should raise Exception, nack, and close. Unlike in an
    integration test, nacked messages are not put back on the queue.
    """
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)

    fake_data = [b'baz-0', b'baz-1', b'baz-2']
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data
    fake_ids = [0, 1, 2]
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

    gen = q.message_generator()  # propagate_error=True
    i = 0
    for msg in gen:
        logging.debug(i)
        assert i < 3
        if i > 0:  # see if previous msg was acked
            mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with(i - 1)

        assert msg is not None
        assert msg.msg_id == i
        assert msg.data == fake_data[i]

        if i == 2:
            with pytest.raises(Exception):
                gen.throw(Exception)
            mock_ap.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(i)
            mock_ap.return_value.close.assert_called()

        i += 1


def test_message_generator_suppress_error(mock_ap: Any, queue_name: str) -> None:
    """Failure-test message generator.

    Generator should not raise Exception. Unlike in an integration test,
    nacked messages are not put back on the queue.
    """
    q = apachepulsar.Backend().create_sub_queue("localhost", queue_name)
    num_msgs = 11
    if num_msgs % 2 == 0:
        raise RuntimeError("`num_msgs` must be odd, so last message is nacked")

    fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]  # type: List[Optional[bytes]]
    fake_data += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

    fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
    fake_ids += [None]  # signifies end of stream -- not actually a message
    mock_ap.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

    gen = q.message_generator(propagate_error=False)
    i = 0
    # odds are acked and evens are nacked
    for msg in gen:
        logging.debug(i)
        if i > 0:
            if i % 2 == 0:  # see if previous EVEN msg was acked
                mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((i - 1) * 10)
            else:  # see if previous ODD msg was NOT acked
                with pytest.raises(AssertionError):
                    mock_ap.return_value.subscribe.return_value.acknowledge.assert_called_with((i - 1) * 10)

        assert msg is not None
        assert msg.msg_id == i * 10
        assert msg.data == fake_data[i]

        if i % 2 == 0:
            gen.throw(Exception)
            mock_ap.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(i * 10)

        i += 1
    mock_ap.return_value.close.assert_called()
