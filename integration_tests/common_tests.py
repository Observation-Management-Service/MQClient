"""
Run integration tests for given backend.

Verify basic functionality.
"""
import uuid
import itertools
from multiprocessing.dummy import Pool as ThreadPool

import pytest

from MQClient import Queue

# don't put in duplicates
DATA_LIST = [{'a': ['foo', 'bar', 3, 4]},
             1,
             '2',
             [1, 2, 3, 4]
             ]


@pytest.fixture
def queue_name():
    name = uuid.uuid4().hex
    print(f"NAME :: {name}")
    return name


class PubSub:
    def test_10(self, queue_name):
        """Test one pub, one sub."""
        pub_sub = Queue(self.backend, name=queue_name)
        pub_sub.send(DATA_LIST[0])

        with pub_sub.recv_one() as d:
            assert d == DATA_LIST[0]

        for d in DATA_LIST:
            pub_sub.send(d)

        for i, d in enumerate(pub_sub.recv(timeout=1)):
            assert d == DATA_LIST[i]


    def test_11(self, queue_name):
        """Test an individual pub and and an individual sub."""
        pub = Queue(self.backend, name=queue_name)
        pub.send(DATA_LIST[0])

        sub = Queue(self.backend, name=queue_name)
        with sub.recv_one() as d:
            assert d == DATA_LIST[0]

        for d in DATA_LIST:
            pub.send(d)

        for i, d in enumerate(sub.recv(timeout=1)):
            assert d == DATA_LIST[i]


    def test_20(self, queue_name):
        """Test one pub, multiple subs, ordered"""
        pub = Queue(self.backend, name=queue_name)

        # for each send, create and receive message via a new sub
        for data in DATA_LIST:
            pub.send(data)
            print(f"SEND :: {data}")

            sub = Queue(self.backend, name=queue_name)
            with sub.recv_one() as d:
                print(f"RECV :: {d}")
                assert d == data
            sub._sub_queue.close()


    def test_21(self, queue_name):
        """Test one pub, multiple subs, unordered"""
        pub = Queue(self.backend, name=queue_name)
        for data in DATA_LIST*10:
            pub.send(data)
            print(f"SEND :: {data}")

        def recv_thread(_):
            sub = Queue(self.backend, name=queue_name)
            return list(sub.recv(timeout=1))

        with ThreadPool(3) as p:
            received_data = p.map(recv_thread, range(3))

        assert len(DATA_LIST)*10 == sum(len(x) for x in received_data)


    def test_30(self, queue_name):
        """Test multiple pubs, one subs."""
        pass


    def test_40(self, queue_name):
        """Test multiple pubs, multiple subs."""
        pass


    def test_50(self, queue_name):
        """Test prefetching"""
        pass
