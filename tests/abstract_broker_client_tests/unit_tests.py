"""Parent class for broker_client unit tests."""

# pylint:disable=invalid-name,protected-access

import logging
from typing import Any, List, Optional
from unittest.mock import Mock

import asyncstdlib as asl
import pytest
from mqclient.broker_client_interface import BrokerClient, Message
from mqclient.queue import Queue

from .utils import is_inst_name


class BrokerClientUnitTest:
    """Unit test suite interface for specified broker_client."""

    broker_client: BrokerClient
    con_patch = ""

    @pytest.fixture
    def mock_con(self, mocker: Any) -> Any:
        """Patch mock_con."""
        return mocker.patch(self.con_patch)

    @staticmethod
    @pytest.fixture
    def queue_name() -> str:
        """Get random queue name."""
        name = Queue.make_name()
        logging.info(f"NAME :: {name}")
        return name

    @staticmethod
    def _get_nack_mock_fn(mock_con: Any) -> Mock:
        """Return mock 'nack' function."""
        raise NotImplementedError()

    @staticmethod
    def _get_ack_mock_fn(mock_con: Any) -> Mock:
        """Return mock 'ack' function."""
        raise NotImplementedError()

    @staticmethod
    def _get_close_mock_fn(mock_con: Any) -> Mock:
        """Return mock 'close' function."""
        raise NotImplementedError()

    @staticmethod
    async def _enqueue_mock_messages(
        mock_con: Any, data: List[bytes], ids: List[int], append_none: bool = True
    ) -> None:
        """Place messages on the mock queue."""
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_ack_message(self, mock_con: Any, queue_name: str) -> None:
        """Test acking message."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False
        await sub.ack_message(Message(12, b""))
        self._get_ack_mock_fn(mock_con).assert_called_with(12)

    @pytest.mark.asyncio
    async def test_reject_message(self, mock_con: Any, queue_name: str) -> None:
        """Test rejecting message."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False
        await sub.reject_message(Message(12, b""))
        self._get_nack_mock_fn(mock_con).assert_called_with(12)

    @pytest.mark.asyncio
    async def test_message_generator_00(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 100

        fake_data = ["baz-{i}".encode("utf-8") for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        await self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        msg: Optional[Message]
        async for i, msg in asl.enumerate(sub.message_generator()):
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                # prev_id = (i - 1) * 10
                # would be called by Queue
                self._get_ack_mock_fn(mock_con).assert_not_called()
            assert msg is not None
            assert msg.msg_id == fake_ids[i]
            assert msg.payload == fake_data[i]

        # last_id = (num_msgs - 1) * 10
        self._get_ack_mock_fn(mock_con).assert_not_called()  # would be called by Queue
        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

    @pytest.mark.asyncio
    async def test_message_generator_01(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b"foo, bar", b"baz"]
        fake_ids = [12, 20]
        await self._enqueue_mock_messages(
            mock_con, fake_data, fake_ids, append_none=False
        )

        m = None
        msg: Optional[Message]
        async for i, msg in asl.enumerate(sub.message_generator()):
            m = msg
            if i == 0:
                break

        assert m is not None
        assert m.msg_id == 12
        assert m.payload == b"foo, bar"
        self._get_ack_mock_fn(mock_con).assert_not_called()  # would be called by Queue
        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

    @pytest.mark.asyncio
    async def test_message_generator_02(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        await self._enqueue_mock_messages(mock_con, [b"foo, bar"], [12])

        m = None
        msg: Optional[Message]
        async for i, msg in asl.enumerate(sub.message_generator()):
            assert i < 1
            m = msg
        assert m is not None
        assert m.msg_id == 12
        assert m.payload == b"foo, bar"
        self._get_ack_mock_fn(mock_con).assert_not_called()  # would be called by Queue
        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

    @pytest.mark.asyncio
    async def test_message_generator_10_upstream_error(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from package code).
        """
        raise NotImplementedError()

    @pytest.mark.asyncio
    async def test_message_generator_20_no_auto_ack(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Test message generator.

        Generator should not ack messages.
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b"baz-0", b"baz-1", b"baz-2"]
        fake_ids = [0, 1, 2]
        await self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        gen = sub.message_generator()
        i = 0
        async for msg in gen:
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                # would be called by Queue
                self._get_ack_mock_fn(mock_con).assert_not_called()

            assert msg is not None
            assert msg.msg_id == i
            assert msg.payload == fake_data[i]

            i += 1

    @pytest.mark.asyncio
    async def test_message_generator_30_propagate_error(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Generator should raise Exception, nack, and close. Unlike in an
        integration test, nacked messages are not put back on the queue.
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b"baz-0", b"baz-1", b"baz-2"]
        fake_ids = [0, 1, 2]
        await self._enqueue_mock_messages(
            mock_con, fake_data, fake_ids, append_none=False
        )

        gen = sub.message_generator()  # propagate_error=True
        i = 0
        async for msg in gen:
            logging.debug(i)
            assert i < 3
            if i > 0:  # see if previous msg was acked
                # would be called by Queue
                self._get_ack_mock_fn(mock_con).assert_not_called()

            assert msg is not None
            assert msg.msg_id == i
            assert msg.payload == fake_data[i]

            if i == 2:
                with pytest.raises(Exception):
                    await gen.athrow(Exception)
                # would be called by Queue
                self._get_nack_mock_fn(mock_con).assert_not_called()
                # would be called by Queue
                self._get_close_mock_fn(mock_con).assert_not_called()

            i += 1

    @pytest.mark.asyncio
    async def test_message_generator_40_suppress_error(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Generator should not raise Exception. Unlike in an integration
        test, nacked messages are not put back on the queue.
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 11
        if num_msgs % 2 == 0:
            raise RuntimeError("`num_msgs` must be odd, so last message is nacked")

        fake_data = [f"baz-{i}".encode("utf-8") for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        await self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        gen = sub.message_generator(propagate_error=False)
        i = 0
        async for msg in gen:
            logging.debug(i)

            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.payload == fake_data[i]

            if i % 2 == 0:
                await gen.athrow(Exception)
                # would be called by Queue
                self._get_nack_mock_fn(mock_con).assert_not_called()

            i += 1

        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

    @pytest.mark.asyncio
    async def test_message_generator_50_consumer_exception_fail(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test message generator.

        Not so much a test, as an example of why QueueSubResource
        is needed.
        """
        sub = await self.broker_client.create_sub_queue("localhost", queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        await self._enqueue_mock_messages(mock_con, [b"baz"], [0], append_none=False)

        excepted = False
        try:
            async for msg in sub.message_generator(propagate_error=False):
                logging.debug(msg)
                raise Exception
        except Exception:
            excepted = True  # QueueSubResource would've suppressed the Exception
        assert excepted

        # would be called by Queue
        self._get_close_mock_fn(mock_con).assert_not_called()

        with pytest.raises(AssertionError):
            self._get_nack_mock_fn(mock_con).assert_called_with(0)

    @pytest.mark.asyncio
    async def test_queue_recv_00_consumer(self, mock_con: Any, queue_name: str) -> None:
        """Test Queue.open_sub()."""
        q = Queue(self.broker_client.NAME, address="localhost", name=queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [Message.serialize("baz")]
        await self._enqueue_mock_messages(mock_con, fake_data, [0])

        async with q.open_sub() as gen:
            async for msg in gen:
                logging.debug(msg)
                assert msg
                assert msg == "baz"

        self._get_close_mock_fn(mock_con).assert_called()
        self._get_ack_mock_fn(mock_con).assert_called_with(0)

    @pytest.mark.asyncio
    async def test_queue_recv_10_comsumer_exception(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test Queue.open_sub().

        When an Exception is raised in `with` block, the Queue should:
        - close (sub) on exit
        - nack the last message
        - suppress the Exception
        """
        q = Queue(self.broker_client.NAME, address="localhost", name=queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [Message.serialize("baz-0"), Message.serialize("baz-1")]
        fake_ids = [0, 1]
        await self._enqueue_mock_messages(
            mock_con, fake_data, fake_ids, append_none=False
        )

        class TestException(Exception):  # pylint: disable=C0115
            pass

        async with q.open_sub() as gen:  # suppress_errors=True
            async for i, msg in asl.enumerate(gen):
                assert i == 0
                logging.debug(msg)
                raise TestException

        self._get_close_mock_fn(mock_con).assert_called()
        self._get_nack_mock_fn(mock_con).assert_called_with(0)

    @pytest.mark.asyncio
    async def test_queue_recv_11_comsumer_exception(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test Queue.open_sub().

        Same as test_queue_recv_10_comsumer_exception() but with multiple
        open_sub() calls.
        """
        q = Queue(self.broker_client.NAME, address="localhost", name=queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 12

        fake_data = [Message.serialize(f"baz-{i}") for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        await self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        async with q.open_sub() as gen:  # suppress_errors=True
            async for msg in gen:
                logging.debug(msg)
                raise TestException

        self._get_close_mock_fn(mock_con).assert_called()
        self._get_nack_mock_fn(mock_con).assert_called_with(0)

        logging.info("Round 2")

        # continue where we left off
        async with q.open_sub() as gen:  # suppress_errors=True
            # self._get_mock_ack(mock_con).assert_not_called()
            async for i, msg in asl.enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    self._get_ack_mock_fn(mock_con).assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            self._get_ack_mock_fn(mock_con).assert_called_with(last_id)

        self._get_close_mock_fn(mock_con).assert_called()

    @pytest.mark.asyncio
    async def test_queue_recv_12_comsumer_exception(
        self, mock_con: Any, queue_name: str
    ) -> None:
        """Failure-test Queue.open_sub().

        Same as test_queue_recv_11_comsumer_exception() but with error
        propagation.
        """
        q = Queue(self.broker_client.NAME, address="localhost", name=queue_name)
        if is_inst_name(
            self.broker_client, "rabbitmq.BrokerClient"
        ):  # HACK: manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 12

        fake_data = [Message.serialize(f"baz-{i}") for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        await self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with pytest.raises(TestException):
            q.except_errors = False
            async with q.open_sub() as gen:
                async for msg in gen:
                    logging.debug(msg)
                    raise TestException

        self._get_close_mock_fn(mock_con).assert_called()
        self._get_nack_mock_fn(mock_con).assert_called_with(0)

        logging.info("Round 2")

        # HACK:- rabbitmq deletes its connection (mock_con) when close()
        # is called, so we need to re-enqueue messages to avoid getting
        # the entire original list.
        # ***Note***: this hack isn't needed in non-mocking tests, see
        # integrate_queue.py integration tests #60+.
        if is_inst_name(q._broker_client, "rabbitmq.BrokerClient"):
            await self._enqueue_mock_messages(mock_con, fake_data[1:], fake_ids[1:])

        # continue where we left off
        q.except_errors = False
        async with q.open_sub() as gen:
            self._get_ack_mock_fn(mock_con).assert_not_called()
            async for i, msg in asl.enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    self._get_ack_mock_fn(mock_con).assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            self._get_ack_mock_fn(mock_con).assert_called_with(last_id)

        self._get_close_mock_fn(mock_con).assert_called()
