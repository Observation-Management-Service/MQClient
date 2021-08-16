"""Parent class for backend unit tests."""

# fmt: off
# pylint:disable=invalid-name,protected-access

import logging
import pickle
from typing import Any, List

import pytest

# local imports
from ..backend_interface import Backend, Message
from ..queue import Queue
from .utils import is_instance_by_name


class BackendUnitTest:
    """Unit test suite interface for specified backend."""

    backend = None  # type: Backend
    con_patch = ''

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
    def _get_mock_nack(mock_con: Any) -> Any:
        """Return mock 'nack' function call."""
        raise NotImplementedError()

    @staticmethod
    def _get_mock_ack(mock_con: Any) -> Any:
        """Return mock 'ack' function call."""
        raise NotImplementedError()

    @staticmethod
    def _get_mock_close(mock_con: Any) -> Any:
        """Return mock 'close' function call."""
        raise NotImplementedError()

    @staticmethod
    def _enqueue_mock_messages(mock_con: Any, data: List[bytes], ids: List[int],
                               append_none: bool = True) -> None:
        """Place messages on the mock queue."""
        raise NotImplementedError()

    def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        raise NotImplementedError()

    def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        raise NotImplementedError()

    def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        raise NotImplementedError()

    def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        raise NotImplementedError()

    def test_ack_message(self, mock_con: Any, queue_name: str) -> None:
        """Test acking message."""
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False
        sub.ack_message(Message(12, b""))
        self._get_mock_ack(mock_con).assert_called_with(12)

    def test_reject_message(self, mock_con: Any, queue_name: str) -> None:
        """Test rejecting message."""
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False
        sub.reject_message(Message(12, b""))
        self._get_mock_nack(mock_con).assert_called_with(12)

    def test_message_generator_00(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 100

        fake_data = ['baz-{i}'.encode('utf-8') for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        for i, msg in enumerate(sub.message_generator()):
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                # prev_id = (i - 1) * 10
                self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue
            assert msg is not None
            assert msg.msg_id == fake_ids[i]
            assert msg.payload == fake_data[i]

        # last_id = (num_msgs - 1) * 10
        self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue
        self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue

    def test_message_generator_01(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b'foo, bar', b'baz']
        fake_ids = [12, 20]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids, append_none=False)

        m = None
        for i, x in enumerate(sub.message_generator()):
            m = x
            if i == 0:
                break

        assert m is not None
        assert m.msg_id == 12
        assert m.payload == b'foo, bar'
        self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue
        self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue

    def test_message_generator_02(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        self._enqueue_mock_messages(mock_con, [b'foo, bar'], [12])

        m = None
        for i, x in enumerate(sub.message_generator()):
            assert i < 1
            m = x
        assert m is not None
        assert m.msg_id == 12
        assert m.payload == b'foo, bar'
        self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue
        self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue

    def test_message_generator_10_upstream_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from package code).
        """
        raise NotImplementedError()

    def test_message_generator_20_no_auto_ack(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator.

        Generator should not ack messages.
        """
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b'baz-0', b'baz-1', b'baz-2']
        fake_ids = [0, 1, 2]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        gen = sub.message_generator()
        i = 0
        for msg in gen:
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue

            assert msg is not None
            assert msg.msg_id == i
            assert msg.payload == fake_data[i]

            i += 1

    def test_message_generator_30_propagate_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception, nack, and close. Unlike in an
        integration test, nacked messages are not put back on the queue.
        """
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [b'baz-0', b'baz-1', b'baz-2']
        fake_ids = [0, 1, 2]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids, append_none=False)

        gen = sub.message_generator()  # propagate_error=True
        i = 0
        for msg in gen:
            logging.debug(i)
            assert i < 3
            if i > 0:  # see if previous msg was acked
                self._get_mock_ack(mock_con).assert_not_called()  # would be called by Queue

            assert msg is not None
            assert msg.msg_id == i
            assert msg.payload == fake_data[i]

            if i == 2:
                with pytest.raises(Exception):
                    gen.throw(Exception)
                self._get_mock_nack(mock_con).assert_not_called()  # would be called by Queue
                self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue

            i += 1

    def test_message_generator_40_suppress_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should not raise Exception. Unlike in an integration
        test, nacked messages are not put back on the queue.
        """
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 11
        if num_msgs % 2 == 0:
            raise RuntimeError("`num_msgs` must be odd, so last message is nacked")

        fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        gen = sub.message_generator(propagate_error=False)
        i = 0
        for msg in gen:
            logging.debug(i)

            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.payload == fake_data[i]

            if i % 2 == 0:
                gen.throw(Exception)
                self._get_mock_nack(mock_con).assert_not_called()  # would be called by Queue

            i += 1
        self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue

    def test_message_generator_50_consumer_exception_fail(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Not so much a test, as an example of why MessageGeneratorContext
        is needed.
        """
        sub = self.backend.create_sub_queue("localhost", queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        self._enqueue_mock_messages(mock_con, [b'baz'], [0], append_none=False)

        excepted = False
        try:
            for msg in sub.message_generator(propagate_error=False):
                logging.debug(msg)
                raise Exception
        except Exception:
            excepted = True  # MessageGeneratorContext would've suppressed the Exception
        assert excepted

        self._get_mock_close(mock_con).assert_not_called()  # would be called by Queue
        with pytest.raises(AssertionError):
            self._get_mock_nack(mock_con).assert_called_with(0)

    def test_queue_recv_00_consumer(self, mock_con: Any, queue_name: str) -> None:
        """Test Queue.recv()."""
        q = Queue(self.backend, address="localhost", name=queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [Message.serialize('baz')]
        self._enqueue_mock_messages(mock_con, fake_data, [0])

        with q.recv() as gen:
            for msg in gen:
                logging.debug(msg)
                assert msg
                assert msg == pickle.loads(fake_data[0])

        self._get_mock_close(mock_con).assert_called()
        self._get_mock_ack(mock_con).assert_called()
        self._get_mock_ack(mock_con).assert_called_with(0)

    def test_queue_recv_10_comsumer_exception(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        When an Exception is raised in `with` block, the Queue should:
        - close (sub) on exit
        - nack the last message
        - suppress the Exception
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        fake_data = [Message.serialize('baz-0'), Message.serialize('baz-1')]
        fake_ids = [0, 1]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids, append_none=False)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # suppress_errors=True
            for i, msg in enumerate(gen):
                assert i == 0
                logging.debug(msg)
                raise TestException

        self._get_mock_close(mock_con).assert_called()
        self._get_mock_nack(mock_con).assert_called_with(0)

    def test_queue_recv_11_comsumer_exception(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        Same as test_queue_recv_10_comsumer_exception() but with multiple
        recv() calls.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 12

        fake_data = [Message.serialize(f'baz-{i}') for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # suppress_errors=True
            for msg in gen:
                logging.debug(msg)
                raise TestException

        self._get_mock_close(mock_con).assert_called()
        self._get_mock_nack(mock_con).assert_called_with(0)

        logging.info("Round 2")

        # continue where we left off
        with q.recv() as gen:  # suppress_errors=True
            # self._get_mock_ack(mock_con).assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    self._get_mock_ack(mock_con).assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            self._get_mock_ack(mock_con).assert_called_with(last_id)

        self._get_mock_close(mock_con).assert_called()

    def test_queue_recv_12_comsumer_exception(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        Same as test_queue_recv_11_comsumer_exception() but with error
        propagation.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        if is_instance_by_name(self.backend, "rabbitmq.Backend"):  # HACK - manually set attr
            mock_con.return_value.is_closed = False

        num_msgs = 12

        fake_data = [Message.serialize(f'baz-{i}') for i in range(num_msgs)]
        fake_ids = [i * 10 for i in range(num_msgs)]
        self._enqueue_mock_messages(mock_con, fake_data, fake_ids)

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with pytest.raises(TestException):
            q.except_errors = False
            with q.recv() as gen:
                for msg in gen:
                    logging.debug(msg)
                    raise TestException

        self._get_mock_close(mock_con).assert_called()
        self._get_mock_nack(mock_con).assert_called_with(0)

        logging.info("Round 2")

        # Hack -- rabbitmq deletes its connection (mock_con) when close()
        # is called, so we need to re-enqueue messages to avoid getting
        # the entire original list.
        # ***Note***: this hack isn't needed in non-mocking tests, see
        # integrate_queue.py integration tests #60+.
        if is_instance_by_name(q._backend, "rabbitmq.Backend"):
            self._enqueue_mock_messages(mock_con, fake_data[1:], fake_ids[1:])

        # continue where we left off
        q.except_errors = False
        with q.recv() as gen:
            self._get_mock_ack(mock_con).assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    self._get_mock_ack(mock_con).assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            self._get_mock_ack(mock_con).assert_called_with(last_id)

        self._get_mock_close(mock_con).assert_called()
