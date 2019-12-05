# import pytest  # type: ignore
import pickle
from functools import partial
from unittest.mock import MagicMock

from MQClient import queue
from MQClient.backend_interface import Message

def test_Queue_init() -> None:
    q = queue.Queue('backend')
    assert q.backend == 'backend'

    q = queue.Queue('backend', name='nnn', address='aaa', prefetch=999)
    assert q.name == 'nnn'
    assert q.address == 'aaa'
    assert q.prefetch == 999

def test_Queue_pub() -> None:
    backend = MagicMock()

    q = queue.Queue(backend)

    raw_q = q.raw_pub_queue
    assert raw_q == backend.create_pub_queue.return_value

def test_Queue_sub() -> None:
    backend = MagicMock()

    q = queue.Queue(backend)

    raw_q = q.raw_sub_queue
    assert raw_q == backend.create_sub_queue.return_value

def test_Queue_send() -> None:
    backend = MagicMock()

    q = queue.Queue(backend)

    data = {'a': 1234}
    q.send(data)

    backend.send_message.assert_called_with(q.raw_pub_queue, pickle.dumps(data, protocol=4))

def gen(data, *args, **kwargs):
    for i, d in enumerate(data):
        yield Message(i, pickle.dumps(d, protocol=4))

def test_Queue_recv() -> None:
    backend = MagicMock()

    q = queue.Queue(backend)

    data = ['a', {'b': 100}, ['foo', 'bar']]
    backend.message_generator.side_effect = partial(gen, data)

    recv_data = list(q.recv())

    assert data == recv_data

def test_Queue_recv_one() -> None:
    backend = MagicMock()

    q = queue.Queue(backend)

    data = {'b': 100}
    msg = Message(0, pickle.dumps(data, protocol=4))
    backend.get_message.return_value = msg

    with q.recv_one() as d:
        recv_data = d

    assert data == recv_data
    backend.ack_message.assert_called_with(q.raw_sub_queue, 0)
