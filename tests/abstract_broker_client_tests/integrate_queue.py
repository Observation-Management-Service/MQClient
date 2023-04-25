"""Run integration tests for given broker_client, on Queue class."""

# pylint:disable=invalid-name,too-many-public-methods,redefined-outer-name,unused-import

import asyncio
import logging
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List

import asyncstdlib as asl
import pytest
from mqclient.queue import Queue, TooManyMessagesPendingAckException

from .utils import (
    DATA_LIST,
    _log_recv,
    _log_recv_multiple,
    _log_send,
    all_were_received,
)


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
    async def test_010(self, queue_name: str, auth_token: str) -> None:
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
    async def test_011(self, queue_name: str, auth_token: str) -> None:
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
    async def test_012(self, queue_name: str, auth_token: str) -> None:
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
    async def test_020(self, queue_name: str, auth_token: str) -> None:
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

    async def _test_021(self, queue_name: str, num_subs: int, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending)."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for data in DATA_LIST:
                await p.send(data)
                _log_send(data)

        async def recv_thread(_: int) -> List[Any]:
            sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
            sub.timeout = 1
            async with sub.open_sub() as gen:
                recv_data_list = [m async for m in gen]
            return _log_recv_multiple(recv_data_list)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(num_subs) as pool:
            received_data = pool.map(start_recv_thread, range(num_subs))
        all_recvd.extend(item for sublist in received_data for item in sublist)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_021_fewer(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Fewer subs than messages.
        """
        await self._test_021(queue_name, len(DATA_LIST) // 2, auth_token)

    @pytest.mark.asyncio
    async def test_021_same(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Same number of subs as messages.
        """
        await self._test_021(queue_name, len(DATA_LIST), auth_token)

    @pytest.mark.asyncio
    async def test_021_more(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        More subs than messages.
        """
        await self._test_021(queue_name, len(DATA_LIST) ** 2, auth_token)

    @pytest.mark.asyncio
    async def test_022(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Use the same number of subs as number of messages.
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
    async def test_023(self, queue_name: str, auth_token: str) -> None:
        """Failure-test one pub, and too many subs.

        More subs than messages with `open_sub_one()` will raise an
        exception.
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
        with pytest.raises(Exception):
            await recv_thread(-1)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_030(self, queue_name: str, auth_token: str) -> None:
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
    async def test_031(self, queue_name: str, auth_token: str) -> None:
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
    async def test_040(self, queue_name: str, auth_token: str) -> None:
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
            all_recvd.extend(_log_recv_multiple(received_data))

            assert len(received_data) == 1
            assert data == received_data[0]

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_041(self, queue_name: str, auth_token: str) -> None:
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
    async def test_042(self, queue_name: str, auth_token: str) -> None:
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
    async def test_043(self, queue_name: str, auth_token: str) -> None:
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
    async def test_050(self, queue_name: str, auth_token: str) -> None:
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
    async def test_051(self, queue_name: str, auth_token: str) -> None:
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
        with pytest.raises(RuntimeError):
            async with recv_gen as gen:
                assert 0  # we should never get here

    @pytest.mark.asyncio
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
    async def test_200__ideal(self, queue_name: str, auth_token: str) -> None:
        """Test open_sub_manual_acking() ideal scenario."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
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
    async def test_210__immediate_recovery(
        self, queue_name: str, auth_token: str
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
    async def test_220__posthoc_recovery(
        self, queue_name: str, auth_token: str
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
    async def test_221__posthoc_recovery__fail(
        self, queue_name: str, auth_token: str
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
    async def test_230__fail_bad_usage(self, queue_name: str, auth_token: str) -> None:
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

    @pytest.mark.asyncio
    async def test_240__delayed_mixed_acking_nacking(
        self, queue_name: str, auth_token: str
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

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        async with sub.open_sub_manual_acking(ack_pending_limit=len(DATA_LIST)) as gen:
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
                        await gen.ack(msg)
                        pending.remove(msg)
                except Exception:
                    await gen.nack(msg)

            for msg in pending:  # messages with index not %2 nor %3, (1,5,7,...)
                await gen.ack(msg)

        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_250__delayed_acking__fail(
        self, queue_name: str, auth_token: str
    ) -> None:
        """Test open_sub_manual_acking() w/ delayed acking AND surpass
        `ack_pending_limit`."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        ack_pending_limit = len(DATA_LIST) // 2
        async with sub.open_sub_manual_acking(ack_pending_limit) as gen:
            messages = []
            with pytest.raises(TooManyMessagesPendingAckException):
                async for i, msg in asl.enumerate(gen.iter_messages()):
                    print(f"{i}: `{msg.data}`")
                    all_recvd.append(_log_recv(msg.data))
                    messages.append(msg)
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                    assert gen._ack_pending == i + 1

        print(all_recvd)
        assert not all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_251__no_nacking__fail(
        self, queue_name: str, auth_token: str
    ) -> None:
        """Test open_sub_manual_acking() w/ delayed acking AND surpass
        `ack_pending_limit`."""
        all_recvd: List[Any] = []

        async with Queue(
            self.broker_client, name=queue_name, auth_token=auth_token
        ).open_pub() as p:
            for d in DATA_LIST:
                await p.send(d)
                _log_send(d)

        sub = Queue(self.broker_client, name=queue_name, auth_token=auth_token)
        sub.timeout = 1
        ack_pending_limit = len(DATA_LIST) // 2
        async with sub.open_sub_manual_acking(ack_pending_limit) as gen:
            messages = []
            with pytest.raises(TooManyMessagesPendingAckException):
                async for i, msg in asl.enumerate(gen.iter_messages()):
                    print(f"{i}: `{msg.data}`")
                    all_recvd.append(_log_recv(msg.data))
                    messages.append(msg)
                    # assert msg.data == DATA_LIST[i]  # we don't guarantee order
                    if i % 2 == 0:
                        pass  # eventually enough "passes" causes a TooManyMessagesPendingAckException
                    else:
                        await gen.ack(msg)
                    assert gen._ack_pending == (i // 2) + 1

        print(all_recvd)
        assert not all_were_received(all_recvd)
