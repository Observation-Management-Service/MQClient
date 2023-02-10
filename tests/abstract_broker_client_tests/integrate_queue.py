"""Run integration tests for given broker_client, on Queue class."""

# pylint:disable=invalid-name,too-many-public-methods,redefined-outer-name,unused-import

import asyncio
import logging
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List

import asyncstdlib as asl
import pytest
from mqclient.queue import Queue

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

    @pytest.mark.asyncio
    async def test_10(self, queue_name: str, auth_token: str) -> None:
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
    async def test_11(self, queue_name: str, auth_token: str) -> None:
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
    async def test_12(self, queue_name: str, auth_token: str) -> None:
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
    async def test_20(self, queue_name: str, auth_token: str) -> None:
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

    async def _test_21(self, queue_name: str, num_subs: int, auth_token: str) -> None:
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

        with ThreadPool(num_subs) as p:
            received_data = p.map(start_recv_thread, range(num_subs))
        all_recvd.extend(item for sublist in received_data for item in sublist)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_21_fewer(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Fewer subs than messages.
        """
        await self._test_21(queue_name, len(DATA_LIST) // 2, auth_token)

    @pytest.mark.asyncio
    async def test_21_same(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Same number of subs as messages.
        """
        await self._test_21(queue_name, len(DATA_LIST), auth_token)

    @pytest.mark.asyncio
    async def test_21_more(self, queue_name: str, auth_token: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        More subs than messages.
        """
        await self._test_21(queue_name, len(DATA_LIST) ** 2, auth_token)

    @pytest.mark.asyncio
    async def test_22(self, queue_name: str, auth_token: str) -> None:
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

        with ThreadPool(len(DATA_LIST)) as p:
            all_recvd = p.map(start_recv_thread, range(len(DATA_LIST)))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_23(self, queue_name: str, auth_token: str) -> None:
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

        with ThreadPool(len(DATA_LIST)) as p:
            all_recvd = p.map(start_recv_thread, range(len(DATA_LIST)))

        # Extra Sub
        with pytest.raises(Exception):
            await recv_thread(-1)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_30(self, queue_name: str, auth_token: str) -> None:
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
    async def test_31(self, queue_name: str, auth_token: str) -> None:
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
    async def test_40(self, queue_name: str, auth_token: str) -> None:
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
    async def test_41(self, queue_name: str, auth_token: str) -> None:
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
    async def test_42(self, queue_name: str, auth_token: str) -> None:
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
    async def test_43(self, queue_name: str, auth_token: str) -> None:
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
    async def test_50(self, queue_name: str, auth_token: str) -> None:
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
    async def test_51(self, queue_name: str, auth_token: str) -> None:
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

    @pytest.mark.asyncio
    async def test_60(self, queue_name: str, auth_token: str) -> None:
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
    async def test_61(self, queue_name: str, auth_token: str) -> None:
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
    async def test_70_fail(self, queue_name: str, auth_token: str) -> None:
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
    async def test_80_break(self, queue_name: str, auth_token: str) -> None:
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
