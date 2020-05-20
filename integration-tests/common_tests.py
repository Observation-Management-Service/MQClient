"""
Run integration tests for given backend.

Verify basic functionality.
"""
from MQClient import Queue


def test_queue(backend):
    q = Queue(backend)
    data = {'a': ['foo', 'bar', 3, 4]}
    q.send(data)

    with q.recv_one() as d:
        assert d == data

    data = [1, '2', data]
    for d in data:
        q.send(d)

    for i, d in enumerate(q.recv(timeout=1)):
        assert d == data[i]
