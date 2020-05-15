"""
Run an integration test for RabbitMQ.

Verify basic functionality.
"""

from MQClient import Queue, backends


def test_queue():
    q = Queue(backends.pulsar)
    data = {'a': ['foo', 'bar', 3, 4]}
    q.send(data)

    with q.recv_one() as d:
        assert d == data

    data = [1, '2', data]
    for d in data:
        q.send(d)

    for i, d in enumerate(q.recv(timeout=1)):
        assert d == data[i]
