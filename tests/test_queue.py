"""Unit test Queue class."""

import pickle
from functools import partial
from typing import Any, Generator, List
from unittest.mock import MagicMock

# local imports
from MQClient import Queue
from MQClient.backend_interface import Backend, Message


def test_Queue_init() -> None:
    """Test constructor."""
    backend = Backend()
    q = Queue(backend)
    assert q.backend == backend

    q = Queue(Backend(), name='nnn', address='aaa', prefetch=999)
    assert q.name == 'nnn'
    assert q.address == 'aaa'
    assert q.prefetch == 999


def test_Queue_pub() -> None:
    """Test pub."""
    backend = MagicMock()
    q = Queue(backend)
    assert q.raw_pub_queue == backend.create_pub_queue.return_value


def test_Queue_sub() -> None:
    """Test sub."""
    backend = MagicMock()
    q = Queue(backend)
    assert q.raw_sub_queue == backend.create_sub_queue.return_value


def test_Queue_send() -> None:
    """Test send."""
    backend = MagicMock()

    q = Queue(backend)

    data = {'a': 1234}
    q.send(data)

    q.raw_pub_queue.send_message.assert_called_with(pickle.dumps(data, protocol=4))  # type: ignore


def test_Queue_recv() -> None:
    """Test recv."""

    def gen(data: List[Any], *args: Any, **kwargs: Any) -> Generator[Message, None, None]:
        for i, d in enumerate(data):
            yield Message(i, pickle.dumps(d, protocol=4))

    backend = MagicMock()

    q = Queue(backend)

    data = ['a', {'b': 100}, ['foo', 'bar']]
    q.raw_sub_queue.message_generator.side_effect = partial(gen, data)  # type: ignore

    recv_data = list(q.recv())

    assert data == recv_data


def test_Queue_recv_one() -> None:
    """Test recv_one."""
    backend = MagicMock()

    q = Queue(backend)

    data = {'b': 100}
    msg = Message(0, pickle.dumps(data, protocol=4))
    q.raw_sub_queue.get_message.return_value = msg  # type: ignore

    with q.recv_one() as d:
        recv_data = d

    assert data == recv_data
    q.raw_sub_queue.ack_message.assert_called_with(0)  # type: ignore
