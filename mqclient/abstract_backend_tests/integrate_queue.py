"""Run integration tests for given backend, on Queue class."""

# pylint:disable=invalid-name,too-many-public-methods

import asyncio
import logging
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, List

import asyncstdlib as asl
import pytest

# local imports
from ..backend_interface import Backend
from ..queue import Queue
from .utils import (
    DATA_LIST,
    _log_recv,
    _log_recv_multiple,
    _log_send,
    all_were_received,
)


class PubSubQueue:
    """Integration test suite for Queue objects."""

    backend = None  # type: Backend

    @pytest.mark.asyncio
    async def test_10(self, queue_name: str) -> None:
        """Test one pub, one sub."""
        all_recvd: List[Any] = []

        pub_sub = Queue(self.backend, name=queue_name)
        await pub_sub.send(DATA_LIST[0])
        _log_send(DATA_LIST[0])

        async with pub_sub.recv_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        for d in DATA_LIST:
            await pub_sub.send(d)
            _log_send(d)

        pub_sub.timeout = 1
        async with pub_sub.recv() as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        assert all_were_received(all_recvd, [DATA_LIST[0]] + DATA_LIST)

    @pytest.mark.asyncio
    async def test_11(self, queue_name: str) -> None:
        """Test an individual pub and an individual sub."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        await pub.send(DATA_LIST[0])
        _log_send(DATA_LIST[0])

        sub = Queue(self.backend, name=queue_name)
        async with sub.recv_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        for d in DATA_LIST:
            await pub.send(d)
            _log_send(d)

        sub.timeout = 1
        async with sub.recv() as gen:
            async for i, d in asl.enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        assert all_were_received(all_recvd, [DATA_LIST[0]] + DATA_LIST)

    @pytest.mark.asyncio
    async def test_12(self, queue_name: str) -> None:
        """Failure-test one pub, two subs (one subscribed to wrong queue)."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        await pub.send(DATA_LIST[0])
        _log_send(DATA_LIST[0])

        sub_fail = Queue(self.backend, name=f"{queue_name}-fail")
        with pytest.raises(Exception) as excinfo:
            async with sub_fail.recv_one() as d:
                all_recvd.append(_log_recv(d))
            assert "No message available" in str(excinfo.value)

        sub = Queue(self.backend, name=queue_name)
        async with sub.recv_one() as d:
            all_recvd.append(_log_recv(d))
            assert d == DATA_LIST[0]

        assert all_were_received(all_recvd, [DATA_LIST[0]])

    @pytest.mark.asyncio
    async def test_20(self, queue_name: str) -> None:
        """Test one pub, multiple subs, ordered/alternatingly."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)

        # for each send, create and receive message via a new sub
        for data in DATA_LIST:
            await pub.send(data)
            _log_send(data)

            sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                all_recvd.append(_log_recv(d))
                assert d == data
            # sub.close() -- no longer needed

        assert all_were_received(all_recvd)

    async def _test_21(self, queue_name: str, num_subs: int) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending)."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        for data in DATA_LIST:
            await pub.send(data)
            _log_send(data)

        async def recv_thread(_: int) -> List[Any]:
            sub = Queue(self.backend, name=queue_name)
            sub.timeout = 1
            async with sub.recv() as gen:
                recv_data_list = list(gen)
            return _log_recv_multiple(recv_data_list)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(num_subs) as p:
            received_data = p.map(start_recv_thread, range(num_subs))
        all_recvd.extend(item for sublist in received_data for item in sublist)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_21_fewer(self, queue_name: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Fewer subs than messages.
        """
        await self._test_21(queue_name, len(DATA_LIST) // 2)

    @pytest.mark.asyncio
    async def test_21_same(self, queue_name: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Same number of subs as messages.
        """
        await self._test_21(queue_name, len(DATA_LIST))

    @pytest.mark.asyncio
    async def test_21_more(self, queue_name: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        More subs than messages.
        """
        await self._test_21(queue_name, len(DATA_LIST) ** 2)

    @pytest.mark.asyncio
    async def test_22(self, queue_name: str) -> None:
        """Test one pub, multiple subs, unordered (front-loaded sending).

        Use the same number of subs as number of messages.
        """
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        for data in DATA_LIST:
            await pub.send(data)
            _log_send(data)

        async def recv_thread(_: int) -> Any:
            sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                recv_data = d
            # sub.close() -- no longer needed
            return _log_recv(recv_data)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(len(DATA_LIST)) as p:
            all_recvd = p.map(start_recv_thread, range(len(DATA_LIST)))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_23(self, queue_name: str) -> None:
        """Failure-test one pub, and too many subs.

        More subs than messages with `recv_one()` will raise an
        exception.
        """
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        for data in DATA_LIST:
            await pub.send(data)
            _log_send(data)

        async def recv_thread(_: int) -> Any:
            sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                recv_data = d
            # sub.close() -- no longer needed
            return _log_recv(recv_data)

        def start_recv_thread(num_id: int) -> Any:
            return asyncio.run(recv_thread(num_id))

        with ThreadPool(len(DATA_LIST)) as p:
            all_recvd = p.map(start_recv_thread, range(len(DATA_LIST)))

        # Extra Sub
        with pytest.raises(Exception) as excinfo:
            recv_thread(0)
            assert "No message available" in str(excinfo.value)

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_30(self, queue_name: str) -> None:
        """Test multiple pubs, one sub, ordered/alternatingly."""
        all_recvd: List[Any] = []

        sub = Queue(self.backend, name=queue_name)

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

            sub.timeout = 1
            sub.except_errors = False
            async with sub.recv() as gen:
                received_data = list(gen)
            all_recvd.extend(_log_recv_multiple(received_data))

            assert len(received_data) == 1
            assert data == received_data[0]

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_31(self, queue_name: str) -> None:
        """Test multiple pubs, one sub, unordered (front-loaded sending)."""
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

        sub = Queue(self.backend, name=queue_name)
        sub.timeout = 1
        async with sub.recv() as gen:
            received_data = list(gen)
        all_recvd.extend(_log_recv_multiple(received_data))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_40(self, queue_name: str) -> None:
        """Test multiple pubs, multiple subs, ordered/alternatingly.

        Use the same number of pubs as subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

            sub = Queue(self.backend, name=queue_name)
            sub.timeout = 1
            async with sub.recv() as gen:
                received_data = list(gen)
            all_recvd.extend(_log_recv_multiple(received_data))

            assert len(received_data) == 1
            assert data == received_data[0]

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_41(self, queue_name: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the same number of pubs as subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

        for _ in range(len(DATA_LIST)):
            sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                all_recvd.append(_log_recv(d))
            # sub.close() -- no longer needed

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_42(self, queue_name: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the more pubs than subs.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

        for i in range(len(DATA_LIST)):
            if i % 2 == 0:  # each sub receives 2 messages back-to-back
                sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                all_recvd.append(_log_recv(d))
            # sub.close() -- no longer needed

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_43(self, queue_name: str) -> None:
        """Test multiple pubs, multiple subs, unordered (front-loaded sending).

        Use the fewer pubs than subs.
        """
        all_recvd: List[Any] = []

        for i, data in enumerate(DATA_LIST):
            if i % 2 == 0:  # each pub sends 2 messages back-to-back
                pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

        for _ in range(len(DATA_LIST)):
            sub = Queue(self.backend, name=queue_name)
            async with sub.recv_one() as d:
                all_recvd.append(_log_recv(d))
            # sub.close() -- no longer needed

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_50(self, queue_name: str) -> None:
        """Test_20 with variable prefetching.

        One pub, multiple subs.
        """
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)

        for i in range(1, len(DATA_LIST) * 2):
            # for each send, create and receive message via a new sub
            for data in DATA_LIST:
                await pub.send(data)
                _log_send(data)

                sub = Queue(self.backend, name=queue_name, prefetch=i)
                async with sub.recv_one() as d:
                    all_recvd.append(_log_recv(d))
                    assert d == data
                # sub.close() -- no longer needed

        assert all_were_received(all_recvd, DATA_LIST * ((len(DATA_LIST) * 2) - 1))

    @pytest.mark.asyncio
    async def test_51(self, queue_name: str) -> None:
        """Test one pub, multiple subs, with prefetching.

        Prefetching should have no visible affect.
        """
        all_recvd: List[Any] = []

        for data in DATA_LIST:
            pub = Queue(self.backend, name=queue_name)
            await pub.send(data)
            _log_send(data)

        # this should not eat up the whole queue
        sub = Queue(self.backend, name=queue_name, prefetch=20)
        async with sub.recv_one() as d:
            all_recvd.append(_log_recv(d))
        async with sub.recv_one() as d:
            all_recvd.append(_log_recv(d))
        # sub.close() -- no longer needed

        sub2 = Queue(self.backend, name=queue_name, prefetch=2)
        sub2.timeout = 1
        async with sub2.recv() as gen:
            for _, d in enumerate(gen):
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_60(self, queue_name: str) -> None:
        """Test recv() fail and recovery, with multiple recv() calls."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        for d in DATA_LIST:
            await pub.send(d)
            _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.backend, name=queue_name)
        sub.timeout = 1
        async with sub.recv() as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                if i == 2:
                    raise TestException()
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order

        logging.warning("Round 2!")

        # continue where we left off
        reused = False
        sub.timeout = 1
        async with sub.recv() as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                reused = True
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order
        assert reused
        print(all_recvd)
        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_61(self, queue_name: str) -> None:
        """Test recv() fail and recovery, with error propagation."""
        all_recvd: List[Any] = []

        pub = Queue(self.backend, name=queue_name)
        for d in DATA_LIST:
            await pub.send(d)
            _log_send(d)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        sub = Queue(self.backend, name=queue_name)
        excepted = False
        try:
            sub.timeout = 1
            sub.except_errors = False
            async with sub.recv() as gen:
                for i, d in enumerate(gen):
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
        async with sub.recv() as gen:
            for i, d in enumerate(gen):
                reused = True
                all_recvd.append(_log_recv(d))
                # assert d == DATA_LIST[i]  # we don't guarantee order
        assert reused

        assert all_were_received(all_recvd)

    @pytest.mark.asyncio
    async def test_70_fail(self, queue_name: str) -> None:
        """Failure-test recv() with reusing a 'MessageAsyncGeneratorContext' instance."""
        pub = Queue(self.backend, name=queue_name)
        for d in DATA_LIST:
            await pub.send(d)
            _log_send(d)

        sub = Queue(self.backend, name=queue_name)
        sub.timeout = 1
        recv_gen = sub.recv()
        async with recv_gen as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                # assert d == DATA_LIST[i]  # we don't guarantee order

        logging.warning("Round 2!")

        # continue where we left off
        with pytest.raises(RuntimeError):
            async with recv_gen as gen:
                assert 0  # we should never get here

    @pytest.mark.asyncio
    async def test_80_break(self, queue_name: str) -> None:
        """Test recv() with a `break` statement."""
        pub = Queue(self.backend, name=queue_name)
        for d in DATA_LIST:
            await pub.send(d)
            _log_send(d)

        sub = Queue(self.backend, name=queue_name)
        sub.timeout = 1
        all_recvd = []
        async with sub.recv() as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))
                if i == 2:
                    break  # NOTE: break is treated as a good exit, so the msg is acked

        logging.warning("Round 2!")

        # continue where we left off
        async with sub.recv() as gen:
            for i, d in enumerate(gen):
                print(f"{i}: `{d}`")
                all_recvd.append(_log_recv(d))

        assert all_were_received(all_recvd)
