"""Run integration tests for given broker_client, on Queue class."""

# pylint:disable=invalid-name,too-many-public-methods,redefined-outer-name,unused-import

import asyncio
import logging
import random
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List, Optional
from unittest.mock import patch

import asyncstdlib as asl
import pytest
from mqclient.broker_client_interface import MQClientException
from mqclient.queue import EmptyQueueException, Queue

from .utils import (
    DATA_LIST,
    _log_recv,
    _log_recv_multiple,
    _log_send,
    all_were_received,
)

#
# retry stuff
#


CI_TEST_RETRY_TRIGGER = "mqclient.broker_clients.utils._ci_test_retry_trigger"


class FailFirstTryException(Exception):
    pass


def fail_first_try(attempt: int) -> None:
    if attempt == 0:
        raise FailFirstTryException()


#
# tests
#


PREFETCH_TEST_VALUES = [None, 1, 2, len(DATA_LIST), 50]


class PubSubQueue:
    """Integration test suite for Queue objects."""

    broker_client: str = ""

    ###########################################################################
    # tests 000 - 099:
    #
    # Testing scenarios with different numbers of sub and/or pubs
    # to see no data loss
    ###########################################################################

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_000(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, one sub."""
        all_recvd: List[Any] = []

        pub_sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        async with pub_sub.open_pub() as p:
            await p.send(DATA_LIST[0])
            _log_send(DATA_LIST[0])

        async with pub_sub.open_sub_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        async with pub_sub.open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        pub_sub.timeout = 1
        async with pub_sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        assert all_were_received(all_recvd, [DATA_LIST[0]] + DATA_LIST)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_001(self, queue_name: str, auth_token: str) -> None:
        """Test an individual pub and an individual sub."""
        all_recvd: List[Any] = []

        pub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        async with pub.open_pub() as p:
            await p.send(DATA_LIST[0])
            _log_send(DATA_LIST[0])

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        async with sub.open_sub_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        async with pub.open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub.timeout = 1
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        assert all_were_received(all_recvd, [DATA_LIST[0]] + DATA_LIST)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_002(self, queue_name: str, auth_token: str) -> None:
        """Failure-test one pub, two subs (one subscribed to wrong queue)."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            await p.send(DATA_LIST[0])
            _log_send(DATA_LIST[0])

        with pytest.raises(Exception):
            name = f"{queue_name}-fail"
            async with Queue(self.broker_client, name=name).open_sub_one() as d:
                all_recvd.append(_log_recv(d))

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_sub_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        assert all_were_received(all_recvd, [DATA_LIST[0]])

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_010(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, ordered/alternatingly."""
        all_recvd: List[Any] = []

        # for each send, create and receive message via a new sub
        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

                async with Queue(
                    self.broker_client, name=queue_name, auth_token=auth_token
                ).open_sub_one() as d:
                    all_recvd.append(_log_recv(d))
                    assert d == data

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    @pytest.mark.parametrize(
        "num_subs",
        [
            len(DATA_LIST) // 2,
            len(DATA_LIST),
            len(DATA_LIST) ** 2,
        ],
    )
    async def test_020(self, queue_name: str, auth_token: str, num_subs: int) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Uses `open_sub()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        subs = []
        for _ in range(num_subs):
            subs.append(
                Queue(
                    self.broker_client,
                    name=queue_name,
                    auth_token=auth_token,
                    timeout=1,
                )
            )
            await asyncio.sleep(0.1)

        for i, sub in enumerate(subs):
            async with sub.open_sub() as gen:
                recv_data_list = [_log_recv(m) async for m in gen]
                if i < len(DATA_LIST):
                    assert recv_data_list
                else:
                    assert not recv_data_list
                all_recvd.extend(recv_data_list)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    @pytest.mark.parametrize(
        "num_subs",
        [
            len(DATA_LIST) // 2,
            len(DATA_LIST),
            len(DATA_LIST) ** 2,
        ],
    )
    async def test_021__threaded(
        self, queue_name: str, auth_token: str, num_subs: int
    ) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Uses `open_sub()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        async def recv_thread(i: int) -> List[Any]:
            sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
            sub.timeout = 1
            async with sub.open_sub() as gen:
                recv_data_list = [m async for m in gen]
            return _log_recv_multiple(recv_data_list)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(num_subs) as pool:
            received_data = pool.map(start_recv_thread, range(num_subs))

        n_subs_that_got_msgs = 0
        for sublist in received_data:
            if sublist:
                n_subs_that_got_msgs += 1
                all_recvd.extend(sublist)
        # since threads are mixed, can't test like test_020
        # the threading makes us not able to assert how many, but it should be >= 1
        logging.debug(f"{n_subs_that_got_msgs=}")
        assert n_subs_that_got_msgs > 2

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_030(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Use the same number of subs as number of messages.

        Uses `open_sub_one()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        subs = [
            Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
                timeout=1,
            )
            for _ in range(len(DATA_LIST))
        ]

        for sub in subs:
            async with sub.open_sub_one() as m:
                all_recvd.append(_log_recv(m))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_031(self, queue_name: str, auth_token: str) -> None:
        """Failure-test one pub, and too many subs.

        More subs than messages with `open_sub_one()` will raise an
        exception.

        Uses `open_sub_one()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        subs = [
            Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
                timeout=1,
            )
            for _ in range(len(DATA_LIST) + 1)
        ]

        for i, sub in enumerate(subs):
            if i == len(DATA_LIST):
                with pytest.raises(EmptyQueueException):
                    async with sub.open_sub_one() as m:
                        all_recvd.append(_log_recv(m))
            else:
                async with sub.open_sub_one() as m:
                    all_recvd.append(_log_recv(m))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_032__threaded(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Use the same number of subs as number of messages.

        Uses `open_sub_one()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        async def recv_thread(_: int) -> Any:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_sub_one() as d:
                recv_data = d
            return _log_recv(recv_data)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(len(DATA_LIST)) as pool:
            all_recvd = pool.map(start_recv_thread, range(len(DATA_LIST)))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_033__threaded(self, queue_name: str, auth_token: str) -> None:
        """Failure-test one pub, and too many subs.

        More subs than messages with `open_sub_one()` will raise an
        exception.

        Uses `open_sub_one()`
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        async def recv_thread(_: int) -> Any:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_sub_one() as d:
                recv_data = d
            return _log_recv(recv_data)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(len(DATA_LIST)) as pool:
            all_recvd = pool.map(start_recv_thread, range(len(DATA_LIST)))

        # Extra Sub
        with pytest.raises(EmptyQueueException):
            await recv_thread(-1)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_060(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, one sub, ordered/alternatingly."""
        all_recvd: List[Any] = []

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

            sub.timeout = 1
            sub.except_errors = False
            async with sub.open_sub() as gen:
                received_data = [m async for m in gen]
            all_recvd.extend(_log_recv_multiple(received_data))

            assert len(received_data) == 1
            assert data == received_data[0]

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_061(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, one sub, unordered (front-loaded sending)."""
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        async with sub.open_sub() as gen:
            received_data = [m async for m in gen]
        all_recvd.extend(_log_recv_multiple(received_data))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_080(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, multiple subs, ordered/alternatingly.

        Use the same number of pubs as subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

            sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
            sub.timeout = 1
            async with sub.open_sub() as gen:
                received_data = [m async for m in gen]
            assert received_data
            all_recvd.extend(_log_recv_multiple(received_data))

            assert len(received_data) == 1
            assert data == received_data[0]

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_081(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the same number of pubs as subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

        for _ in range(len(DATA_LIST)):
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_sub_one() as d:
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_082(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the more pubs than subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

        for i in range(len(DATA_LIST)):
            if i % 2 == 0:  # each sub receives 2 messages back-to-back
                sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
            async with sub.open_sub_one() as d:
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_083(self, queue_name: str, auth_token: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the fewer pubs than subs.
        """
        all_recvd: List[Any] = []

        for data_pairs in [DATA_LIST[i : i + 2] for i in range(0, len(DATA_LIST), 2)]:
            for data in data_pairs:
                async with Queue(
                    self.broker_client, name=queue_name, auth_token=auth_token
                ).open_pub() as p:
                    await p.send(data)
                    _log_send(data)

        for _ in range(len(DATA_LIST)):
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_sub_one() as d:
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_090(self, queue_name: str, auth_token: str) -> None:
        """Test_20 with variable prefetching.

        One pub, multiple subs.
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for i in range(1, len(DATA_LIST) * 2):
                # for each send, create and receive message via a new sub
                for data in DATA_LIST:
                    await p.send(data)
                    _log_send(data)

                    sub = Queue(
                        self.broker_client,
                        name=queue_name,
                        auth_token=auth_token,
                        prefetch=i,
                    )
                    async with sub.open_sub_one() as d:
                        all_recvd.append(_log_recv(d))
                        assert d == data

        assert all_were_received(all_recvd, DATA_LIST * ((len(DATA_LIST) * 2) - 1))

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_091(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, with prefetching.

        Prefetching should have no visible affect.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            async with Queue(
                self.broker_client, name=queue_name, auth_token=auth_token
            ).open_pub() as p:
                await p.send(data)
                _log_send(data)

        # this should not eat up the whole queue
        sub = Queue(
            self.broker_client, name=queue_name, auth_token=auth_token, prefetch=20
        )
        async with sub.open_sub_one() as d:
            all_recvd.append(_log_recv(d))
        async with sub.open_sub_one() as d:
            all_recvd.append(_log_recv(d))

        sub2 = Queue(
            self.broker_client, name=queue_name, auth_token=auth_token, prefetch=2
        )
        sub2.timeout = 1
        async with sub2.open_sub() as gen:
            async for _, d in asl.enumerate(gen):
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    ###########################################################################
    # tests 100 - 199:
    #
    # Tests for open_sub()
    ###########################################################################

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_100(self, queue_name: str, auth_token: str) -> None:
        """Test open_sub() fail and recovery, with multiple open_sub()
        calls."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                if i == 2:
                    raise TestException()
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        logging.warning("Round 2!")

        # continue where we left off
        reused = False
        sub.timeout = 1
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                reused = True
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order
        assert reused
        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_101(self, queue_name: str, auth_token: str) -> None:
        """Test open_sub() fail and recovery, with error propagation."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        excepted = False
        try:
            sub.timeout = 1
            sub.except_errors = False
            async with sub.open_sub() as gen:
                async for i, d in asl.enumerate(gen):
                    if i == 2:
                        raise TestException()
                    all_recvd.append(_log_recv(d))
                    # assert d == DATA_LIST[i]  # we don't guarantee order
        except TestException:
            excepted = True
        assert excepted

        logging.warning("Round 2!")

        # continue where we left off
        reused = False
        sub.timeout = 1
        sub.except_errors = False
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                reused = True
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order
        assert reused

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_110__fail(self, queue_name: str, auth_token: str) -> None:
        """Failure-test open_sub() with reusing a 'QueueSubResource'
        instance."""
        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        recv_gen = sub.open_sub()
        async with recv_gen as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                # assert d == DATA_LIST[i]  # we don't guarantee order

        logging.warning("Round 2!")

        # continue where we left off
        with pytest.raises(MQClientException):
            async with recv_gen as gen:
                assert 0  # we should never get here

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_120_break(self, queue_name: str, auth_token: str) -> None:
        """Test open_sub() with a `break` statement."""
        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        all_recvd = []
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                if i == 2:
                    break  # NOTE: break is treated as a good exit, so the msg is acked

        logging.warning("Round 2!")

        # continue where we left off
        async with sub.open_sub() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    ###########################################################################
    # tests 200 - 299:
    #
    # Tests for open_sub_manual_acking()
    ###########################################################################

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    @pytest.mark.parametrize("sub_queue_prefetch", PREFETCH_TEST_VALUES)
    async def test_200__ideal(
        self,
        queue_name: str,
        auth_token: str,
        sub_queue_prefetch: Optional[int],
    ) -> None:
        """Test open_sub_manual_acking() ideal scenario."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        if sub_queue_prefetch is not None:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
                prefetch=sub_queue_prefetch,
            )
        else:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
            )
        sub.timeout = 1
        async with sub.open_sub_manual_acking() as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                print(f"{i}: `{msg.data}`")
                all_recvd.append(_log_recv(msg.data))
                # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                await gen.ack(msg)

        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    @pytest.mark.parametrize("sub_queue_prefetch", PREFETCH_TEST_VALUES)
    async def test_202__delayed_mixed_acking_nacking(
        self,
        queue_name: str,
        auth_token: str,
        sub_queue_prefetch: Optional[int],
    ) -> None:
        """Test open_sub_manual_acking() fail and immediate recovery with
        multi-tasking, with mixed acking and nacking."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        if sub_queue_prefetch is not None:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
                prefetch=sub_queue_prefetch,
            )
        else:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
            )
        sub.timeout = 1
        async with sub.open_sub_manual_acking() as gen:
            pending = []
            async for i, msg in asl.enumerate(gen.iter_messages()):
                try:
                    # DO WORK!
                    print(f"{i}: `{msg.data}`")
                    if i % 3 == 0:  # nack every 1/3
                        raise TestException()
                    all_recvd.append(_log_recv(msg.data))
                    pending.append(msg)
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                    if i % 2 == 0:  # ack every 1/2
                        print(f"ack {i}: `{msg.data}`")
                        await gen.ack(msg)
                        pending.remove(msg)
                except Exception:
                    print(f"nack {i}: `{msg.data}`")
                    await gen.nack(msg)

            for msg in pending:  # messages with index not %2 nor %3, (1,5,7,...)
                await gen.ack(msg)

        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    @pytest.mark.parametrize("sub_queue_prefetch", PREFETCH_TEST_VALUES)
    async def test_204__post_ack(
        self,
        queue_name: str,
        auth_token: str,
        sub_queue_prefetch: Optional[int],
    ) -> None:
        """Test open_sub_manual_acking() where messages aren't acked until
        after all have been received."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        if sub_queue_prefetch is not None:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
                prefetch=sub_queue_prefetch,
            )
        else:
            sub = Queue(
                self.broker_client,
                name=queue_name,
                auth_token=auth_token,
            )
        sub.timeout = 1
        to_ack = []
        async with sub.open_sub_manual_acking() as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                print(f"{i}: `{msg.data}`")
                all_recvd.append(_log_recv(msg.data))
                to_ack.append(msg)
                # assert msg.data == DATA_LIST[i]  # we don't guarantee order

            iter_em = list(enumerate(to_ack))
            random.shuffle(iter_em)
            for i, msg in iter_em:
                print(f"ack {i} (shuffled): `{msg.data}`")
                await gen.ack(msg)

        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_210__immediate_recovery(
        self,
        queue_name: str,
        auth_token: str,
    ) -> None:
        """Test open_sub_manual_acking() fail and immediate recovery, with
        nacking."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        async with sub.open_sub_manual_acking() as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                try:
                    # DO WORK!
                    print(f"{i}: `{msg.data}`")
                    if i == 2:
                        raise TestException()
                    all_recvd.append(_log_recv(msg.data))
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                except Exception:
                    await gen.nack(msg)
                else:
                    await gen.ack(msg)

        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_220__posthoc_recovery(
        self,
        queue_name: str,
        auth_token: str,
    ) -> None:
        """Test open_sub_manual_acking() fail and post-hoc recovery, with
        nacking."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        excepted = False
        sub.timeout = 1
        # sub.except_errors = False  # has no effect
        async with sub.open_sub_manual_acking() as gen:
            try:
                async for i, msg in asl.enumerate(gen.iter_messages()):
                    print(f"{i}: `{msg.data}`")
                    if i == 2:
                        raise TestException()
                    all_recvd.append(_log_recv(msg.data))
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                    await gen.ack(msg)
            except TestException:
                excepted = True
                await gen.nack(msg)
        assert excepted

        logging.warning("Round 2!")

        # continue where we left off
        posthoc = False
        sub.timeout = 1
        async with sub.open_sub_manual_acking() as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                print(f"{i}: `{msg.data}`")
                posthoc = True
                all_recvd.append(_log_recv(msg.data))
                # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                await gen.ack(msg)
        assert posthoc
        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_221__posthoc_recovery__fail(
        self,
        queue_name: str,
        auth_token: str,
    ) -> None:
        """Test open_sub_manual_acking() fail, post-hoc recovery, then fail.

        Final fail is due to not nacking.
        """
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        errored_msg = None

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        excepted = False
        async with sub.open_sub_manual_acking() as gen:
            try:
                async for i, msg in asl.enumerate(gen.iter_messages()):
                    print(f"{i}: `{msg.data}`")
                    if i == 2:
                        errored_msg = msg.data
                        raise TestException()
                    all_recvd.append(_log_recv(msg.data))
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                    await gen.ack(msg)
            except TestException:
                excepted = True
                # await gen.nack(msg)  # no acking
        assert excepted

        logging.warning("Round 2!")

        # continue where we left off
        posthoc = False
        sub.timeout = 1
        async with sub.open_sub_manual_acking() as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                print(f"{i}: `{msg.data}`")
                posthoc = True
                all_recvd.append(_log_recv(msg.data))
                # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                await gen.ack(msg)
        assert posthoc

        # Either all the messages have been gotten (re-opening the connection took longer enough)
        # OR it hasn't been long enough to redeliver un-acked/nacked message
        # This is difficult to test -- all we can tell is if it is one of these scenarios
        print(all_recvd)
        assert all_were_received(all_recvd) or (
            all_were_received(all_recvd + [errored_msg])
        )

    @pytest.mark.asyncio
    @patch(CI_TEST_RETRY_TRIGGER, new=fail_first_try)
    async def test_230__fail_bad_usage(
        self,
        queue_name: str,
        auth_token: str,
    ) -> None:
        """Failure-test open_sub_manual_acking() with reusing a
        'QueueSubResource' instance."""
        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        recv_gen = sub.open_sub_manual_acking()
        async with recv_gen as gen:
            async for i, msg in asl.enumerate(gen.iter_messages()):
                print(f"{i}: `{msg.data}`")
                # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                await gen.ack(msg)

        logging.warning("Round 2!")

        # continue where we left off
        with pytest.raises((AttributeError, RuntimeError)):
            # AttributeError: '_AsyncGeneratorContextManager' object has no attribute 'args'
            # RuntimeError: generator didn't yield
            async with recv_gen as gen:
                assert 0  # we should never get here
