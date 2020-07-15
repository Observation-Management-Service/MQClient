"""Run integration tests for given backend, on backend_interface classes.

Verify functionality that is abstracted away from the Queue class.
"""

# pylint: disable=redefined-outer-name

import itertools
import logging
import pickle
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List

import pytest  # type: ignore

# local imports
from MQClient.backend_interface import Backend, Message

from .utils import DATA_LIST, _print_recv, _print_recv_multiple, _print_send


def _print_recv_message(recv_msg: Message) -> str:
    recv_data = None
    if recv_msg:
        recv_data = pickle.loads(recv_msg.data)
    _print_recv(f"{recv_msg} -> {recv_data}")


class PubSubBackendInterface:
    """Integration test suite for backend_interface objects.

    Only test things that cannot be tested via the Queue class.
    """

    backend = None  # type: Backend

    def test_00(self, queue_name: str) -> None:
        """Sanity test."""
        pub = self.backend.create_pub_queue('localhost', queue_name)
        sub = self.backend.create_sub_queue('localhost', queue_name)

        # send
        for msg in DATA_LIST:
            raw_data = pickle.dumps(msg, protocol=4)
            pub.send_message(raw_data)
            _print_send(msg)

        # receive
        for i in itertools.count():
            print(i)
            assert i <= len(DATA_LIST)
            recv_msg = sub.get_message()
            _print_recv_message(recv_msg)

            # check received message
            if i == len(DATA_LIST):
                assert not recv_msg  # None signifies end of stream
                break
            assert recv_msg
            assert DATA_LIST[i] == pickle.loads(recv_msg.data)

    def test_10(self, queue_name: str) -> None:
        """Test nacking."""
        pub = self.backend.create_pub_queue('localhost', queue_name)
        sub = self.backend.create_sub_queue('localhost', queue_name)

        # send
        for msg in DATA_LIST:
            raw_data = pickle.dumps(msg, protocol=4)
            pub.send_message(raw_data)
            _print_send(msg)

        # receive -- nack each message, once
        for i in itertools.count():
            print(i)
            assert i <= len(DATA_LIST) * 2
            recv_msg = sub.get_message()
            _print_recv_message(recv_msg)

            # check received message
            if i == len(DATA_LIST) * 2:
                assert not recv_msg  # None signifies end of stream
                break
            assert recv_msg
            assert DATA_LIST[i//2] == pickle.loads(recv_msg.data)

            if i % 2 == 0:
                sub.reject_message(recv_msg.msg_id)
                print('NACK!')
