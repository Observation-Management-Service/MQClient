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
        print(f"OUT!! {d}")
        assert d == data

    data = [1, '2', data]
    for d in data:
        print(f"SEND:: {d}")
        q.send(d)

    for i, d in enumerate(q.recv(timeout=1)):
        print(f"RECV:: {i}:{d}")
        assert d == data[i]
