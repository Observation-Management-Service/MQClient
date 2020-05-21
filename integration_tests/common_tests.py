"""Run integration tests for given backend.

Verify basic functionality.
"""

# pylint: disable=redefined-outer-name

import typing  # pylint: disable=W0611
import uuid
from multiprocessing.dummy import Pool as ThreadPool

import pytest  # type: ignore
from MQClient import Queue

# don't put in duplicates
DATA_LIST = [{'a': ['foo', 'bar', 3, 4]},
             1,
             '2',
             [1, 2, 3, 4]
             ]


@pytest.fixture
def queue_name():
    """Get random queue name."""
    name = uuid.uuid4().hex
    print(f"NAME :: {name}")
    return name


class PubSub:
    """Integration test suite."""

    backend = None  # type: typing.Any

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
        """Test one pub, multiple subs, ordered."""
        pub = Queue(self.backend, name=queue_name)

        # for each send, create and receive message via a new sub
        for data in DATA_LIST:
            pub.send(data)
            print(f"SEND :: {data}")

            sub = Queue(self.backend, name=queue_name)
            with sub.recv_one() as d:
                print(f"RECV :: {d}")
                assert d == data
            sub.close()

    def test_21(self, queue_name):
        """Test one pub, multiple subs, unordered.

        Repeat process with increasing number of subs, so the ratio of messages and subs vary.
        """
        for i in range(1, len(DATA_LIST)):
            num_subs = i**2

            pub = Queue(self.backend, name=queue_name)
            for data in DATA_LIST:
                pub.send(data)
                print(f"SEND :: {data}")

            def recv_thread(_):
                sub = Queue(self.backend, name=queue_name)
                recv_data_list = list(sub.recv(timeout=1))
                print(f"RECV - {len(recv_data_list)} :: {recv_data_list}")
                return recv_data_list

            with ThreadPool(num_subs) as p:
                received_data = p.map(recv_thread, range(num_subs))
            received_data = [item for sublist in received_data for item in sublist]

            assert len(DATA_LIST) == len(received_data)
            for data in DATA_LIST:
                assert data in received_data

    def test_30(self, queue_name):
        """Test multiple pubs, one subs."""
        pass

    def test_40(self, queue_name):
        """Test multiple pubs, multiple subs."""
        pass

    def test_50(self, queue_name):
        """Test prefetching."""
        pass
