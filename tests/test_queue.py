"""Unit test Queue class."""

# fmt: off
# pylint:disable=invalid-name,protected-access

from functools import partial
from typing import Any, Generator, List
from unittest.mock import MagicMock

# local imports
from mqclient.backend_interface import Backend, Message
from mqclient.queue import Queue


def test_init() -> None:
    """Test constructor."""
    backend = Backend()
    q = Queue(backend)
    assert q._backend == backend

    q = Queue(Backend(), name='nnn', address='aaa', prefetch=999)
    assert q._name == 'nnn'
    assert q._address == 'aaa'
    assert q._prefetch == 999


def test_pub() -> None:
    """Test pub."""
    backend = MagicMock()
    q = Queue(backend)
    assert q.raw_pub_queue == backend.create_pub_queue.return_value


def test_send() -> None:
    """Test send."""
    backend = MagicMock()

    q = Queue(backend)

    data = {'a': 1234}
    q.send(data)

    q.raw_pub_queue.send_message.assert_called_with(Message.serialize_data(data))  # type: ignore


def test_recv() -> None:
    """Test recv."""

    def gen(data: List[Any], *args: Any, **kwargs: Any) -> Generator[Message, None, None]:
        for i, d in enumerate(data):
            yield Message(i, Message.serialize_data(d))

    backend = MagicMock()

    q = Queue(backend)

    data = ['a', {'b': 100}, ['foo', 'bar']]
    # q.raw_sub_queue.message_generator.side_effect = partial(gen, data)  # type: ignore
    backend.create_sub_queue.return_value.message_generator.side_effect = partial(gen, data)

    with q.recv() as recv_gen:
        recv_data = list(recv_gen)
        assert data == recv_data


def test_recv_one() -> None:
    """Test recv_one."""
    backend = MagicMock()

    q = Queue(backend)

    data = {"b": 100}
    msg = Message(0, Message.serialize_data(data))
    backend.create_sub_queue.return_value.get_message.return_value = msg

    with q.recv_one() as d:
        recv_data = d

    assert data == recv_data
    backend.create_sub_queue.return_value.ack_message.assert_called_with(msg)
