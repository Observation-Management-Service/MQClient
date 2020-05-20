"""
Run integration tests for given backend.

Verify basic functionality.
"""
import uuid

from MQClient import Queue

# don't put in duplicates
DATA_LIST = [{'a': ['foo', 'bar', 3, 4]},
             1,
             '2',
             [1, 2, 3, 4]
             ]


def _new_queue_name():
    name = uuid.uuid4().hex
    print(f"NAME :: {name}")
    return name


def test_10(backend):
    """Test one pub, one sub."""
    pub_sub = Queue(backend)
    pub_sub.send(DATA_LIST[0])

    with pub_sub.recv_one() as d:
        assert d == DATA_LIST[0]

    for d in DATA_LIST:
        pub_sub.send(d)

    for i, d in enumerate(pub_sub.recv(timeout=1)):
        assert d == DATA_LIST[i]


def test_11(backend):
    """Test an individual pub and and an individual sub."""
    name = _new_queue_name()

    # pub = Queue(backend, name)
    # pub.send(DATA_LIST[0])

    # sub = Queue(backend, name)
    # with sub.recv_one() as d:
    #     assert d == DATA_LIST[0]

    # for d in DATA_LIST:
    #     pub.send(d)

    # for i, d in enumerate(sub.recv(timeout=1)):
    #     assert d == DATA_LIST[i]


def test_20(backend):
    """Test one pub, multiple subs."""
    name = _new_queue_name()

    pub = Queue(backend, name=name)

    # for each send, create and receive message via a new sub
    for data in DATA_LIST:
        pub.send(data)
        print(f"SEND :: {data}")

        sub = Queue(backend, name=name)
        with sub.recv_one() as d:
            print(f"RECV :: {d}")
            assert d == data


def test_21(backend):
    """Test one pub, multiple subs."""
    name = _new_queue_name()

    pub = Queue(backend, name=name)
    for data in DATA_LIST:
        pub.send(data)
        print(f"SEND :: {data}")

    recevied_data = []
    for _ in DATA_LIST:
        sub = Queue(backend, name=name)
        with sub.recv_one() as d:
            print(f"RECV :: {d}")
            assert (d in DATA_LIST) and (d not in recevied_data)
            recevied_data.append(d)


def test_30(backend):
    """Test multiple pubs, one subs."""
    name = _new_queue_name()


def test_40(backend):
    """Test multiple pubs, multiple subs."""
    name = _new_queue_name()


def test_50(backend):
    """Test prefetching"""
    pass
