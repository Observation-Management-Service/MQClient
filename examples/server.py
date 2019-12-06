"""
A server sends work out on one queue, and receives results on another.
"""
import typing

from MQClient import backends, Queue

def server(work_queue: Queue, result_queue: Queue) -> None:
    for i in range(100):
        m = {'id': i, 'cmd': f'echo "{i}"'}
        work_queue.send(m)

    results = {}
    for m in result_queue.recv(timeout=5):
        results[typing.cast(int, m['id'])] = typing.cast(str, m['out'])

    print(results)
    assert len(results) == 100
    for i in results:
        assert results[i].strip() == str(i)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--address', default='localhost', help='queue address')
    parser.add_argument('--work-queue', default='queue1', help='work queue')
    parser.add_argument('--result-queue', default='queue2', help='result queue')
    parser.add_argument('--prefetch', type=int, default=10, help='result queue prefetch')
    args = parser.parse_args()

    workq = Queue(backends.rabbitmq, args.address, args.work_queue)
    resultq = Queue(backends.rabbitmq, args.address, args.result_queue, args.prefetch)

    server(workq, resultq)
