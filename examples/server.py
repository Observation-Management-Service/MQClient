"""A server sends work out on one queue, and receives results on another."""

import typing

# local imports
from MQClient import Queue, backends


def server(work_queue: Queue, result_queue: Queue) -> None:
    """Demo example server."""
    for i in range(100):
        m = {'id': i, 'cmd': f'echo "{i}"'}
        work_queue.send(m)

    results = {}
    for data in result_queue.recv(timeout=5):
        assert isinstance(data, dict)
        results[typing.cast(int, data['id'])] = typing.cast(str, data['cmd'])

    print(results)
    assert len(results) == 100
    for i, res in enumerate(results):
        assert res == i
        assert results[res] == f'echo "{i}"'


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--address', default='localhost', help='queue address')
    parser.add_argument('--queue', default='queue', help='name of global queue')
    parser.add_argument('--prefetch', type=int, default=10, help='result queue prefetch')
    args = parser.parse_args()

    backend = backends.rabbitmq.Backend()
    workq = Queue(backend, address=args.address, name=args.queue)
    resultq = Queue(backend, address=args.address, name=args.queue, prefetch=args.prefetch)

    server(workq, resultq)
