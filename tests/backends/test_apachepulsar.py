"""Unit Tests for Pulsar Backend."""

import logging
import pickle
from typing import Any, List, Optional

import pytest  # type: ignore

# local imports
from MQClient import Queue
from MQClient.backends import apachepulsar

from .common_unit_tests import BackendUnitTest


class TestUnitApachePulsar(BackendUnitTest):
    """Unit test suite interface for Apache Pulsar backend."""

    backend = apachepulsar.Backend()
    con_patch = 'pulsar.Client'

    def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        assert q.topic == queue_name
        mock_con.return_value.create_producer.assert_called()

    def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        q = self.backend.create_sub_queue("localhost", queue_name, prefetch=213)
        assert q.topic == queue_name
        assert q.prefetch == 213
        mock_con.return_value.subscribe.assert_called()

    def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        q.send_message(b"foo, bar, baz")
        mock_con.return_value.create_producer.return_value.send.assert_called_with(b'foo, bar, baz')

    def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        mock_con.return_value.subscribe.return_value.receive.return_value.data.return_value = b'foo, bar'
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.return_value = 12
        m = q.get_message()

        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'

    def test_ack_message(self, mock_con: Any, queue_name: str) -> None:
        """Test acking message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        q.ack_message(12)
        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(12)

    def test_reject_message(self, mock_con: Any, queue_name: str) -> None:
        """Test rejecting message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        q.reject_message(12)
        mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(12)

    def test_message_generator_0(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        num_msgs = 100

        fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]  # type: List[Optional[bytes]]
        fake_data += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

        fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
        fake_ids += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        for i, msg in enumerate(q.message_generator()):
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                prev_id = (i - 1) * 10
                mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(prev_id)
            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.data == fake_data[i]

        last_id = (num_msgs - 1) * 10
        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(last_id)
        mock_con.return_value.close.assert_called()

    def test_message_generator_1(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
            b'foo, bar', b'baz']
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
            12, 20]

        m = None
        for i, x in enumerate(q.message_generator()):
            m = x
            if i == 0:
                break

        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'

        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
        mock_con.return_value.close.assert_called()

    def test_message_generator_2(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = [
            b'foo, bar',
            None  # signifies end of stream -- not actually a message
        ]
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [
            12,
            None  # signifies end of stream -- not actually a message
        ]

        m = None
        for i, x in enumerate(q.message_generator()):
            assert i < 1
            m = x

        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'

        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(12)
        mock_con.return_value.close.assert_called()

    def test_message_generator_upstream_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pulsar-package code).
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        mock_con.return_value.subscribe.return_value.receive.side_effect = Exception()
        with pytest.raises(Exception):
            _ = list(q.message_generator())
        mock_con.return_value.close.assert_called()

        # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
        mock_con.return_value.subscribe.return_value.receive.side_effect = Exception()
        with pytest.raises(Exception):
            _ = list(q.message_generator(propagate_error=False))
        mock_con.return_value.close.assert_called()

    def test_message_generator_no_auto_ack(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator.

        Generator should not ack messages.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        fake_data = [b'baz-0', b'baz-1', b'baz-2']  # type: List[Optional[bytes]]
        fake_data += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data
        fake_ids = [0, 1, 2]  # type: List[Optional[int]]
        fake_ids += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        gen = q.message_generator(auto_ack=False)
        i = 0
        for msg in gen:
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                mock_con.return_value.subscribe.return_value.acknowledge.assert_not_called()

            assert msg is not None
            assert msg.msg_id == i
            assert msg.data == fake_data[i]

            i += 1

    def test_message_generator_propagate_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception, nack, and close. Unlike in an
        integration test, nacked messages are not put back on the queue.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        fake_data = [b'baz-0', b'baz-1', b'baz-2']
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data
        fake_ids = [0, 1, 2]
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        gen = q.message_generator()  # propagate_error=True
        i = 0
        for msg in gen:
            logging.debug(i)
            assert i < 3
            if i > 0:  # see if previous msg was acked
                mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(i - 1)

            assert msg is not None
            assert msg.msg_id == i
            assert msg.data == fake_data[i]

            if i == 2:
                with pytest.raises(Exception):
                    gen.throw(Exception)
                mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(i)
                mock_con.return_value.close.assert_called()

            i += 1

    def test_message_generator_suppress_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should not raise Exception. Unlike in an integration
        test, nacked messages are not put back on the queue.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)
        num_msgs = 11
        if num_msgs % 2 == 0:
            raise RuntimeError("`num_msgs` must be odd, so last message is nacked")

        fake_data = [f'baz-{i}'.encode('utf-8') for i in range(num_msgs)]  # type: List[Optional[bytes]]
        fake_data += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

        fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
        fake_ids += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        gen = q.message_generator(propagate_error=False)
        i = 0
        # odds are acked and evens are nacked
        for msg in gen:
            logging.debug(i)
            if i > 0:
                prev_id = (i - 1) * 10
                if i % 2 == 0:  # see if previous EVEN msg was acked
                    mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(prev_id)
                else:  # see if previous ODD msg was NOT acked
                    with pytest.raises(AssertionError):
                        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(prev_id)

            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.data == fake_data[i]

            if i % 2 == 0:
                gen.throw(Exception)
                mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(i * 10)

            i += 1

        mock_con.return_value.close.assert_called()

    def test_message_generator_consumer_exception_0(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Not so much a test, as an example of why MessageGeneratorContext
        is needed.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = [b'baz']
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = [0]

        excepted = False
        try:
            for msg in q.message_generator(propagate_error=False):
                logging.debug(msg)
                raise Exception
        except Exception:
            excepted = True  # MessageGeneratorContext would've suppressed the Exception
        assert excepted

        # MessageGeneratorContext would've guaranteed both of these
        with pytest.raises(AssertionError):
            mock_con.return_value.close.assert_not_called()
        with pytest.raises(AssertionError):
            mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(0)

    def test_queue_recv_consumer(self, mock_con: Any, queue_name: str) -> None:
        """Test Queue.recv()."""
        q = Queue(self.backend, address="localhost", name=queue_name)

        data = [pickle.dumps('baz', protocol=4), None]
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = data
        ids = [0, None]
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = ids

        with q.recv() as gen:
            for msg in gen:
                logging.debug(msg)
                assert data
                assert msg == pickle.loads(data[0])  # type: ignore

        mock_con.return_value.close.assert_called()
        mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(0)

    def test_queue_recv_comsumer_exception_0(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        When an Exception is raised in `with` block, the Queue should:
        - NOT close (pub) on exit
        - nack the message
        - suppress the Exception
        """
        q = Queue(self.backend, address="localhost", name=queue_name)

        fake_data = [pickle.dumps('baz-0', protocol=4), pickle.dumps('baz-1', protocol=4)]
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

        fake_ids = [0, 1]
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # propagate_error=False
            for i, msg in enumerate(gen):
                assert i == 0
                logging.debug(msg)
                raise TestException

        mock_con.return_value.close.assert_not_called()
        mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(0)

    def test_queue_recv_comsumer_exception_1(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        The Queue.recv()'s context manager should be reusable after
        suppressing an Exception.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        num_msgs = 12

        fake_data = [pickle.dumps(f'baz-{i}', protocol=4) for i in range(num_msgs)]  # type: List[Optional[bytes]]
        fake_data += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

        fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
        fake_ids += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        class TestException(Exception):  # pylint: disable=C0115
            pass

        g = q.recv()
        with g as gen:  # propagate_error=False
            for msg in gen:
                logging.debug(msg)
                raise TestException

        mock_con.return_value.close.assert_not_called()
        mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(0)

        logging.info("Round 2")

        with g as gen:  # propagate_error=False
            mock_con.return_value.subscribe.return_value.acknowledge.assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(last_id)

        mock_con.return_value.close.assert_called()

    def test_queue_recv_comsumer_exception_2(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        Same as test_queue_recv_comsumer_exception_1() but with multiple
        recv() calls.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        num_msgs = 12

        fake_data = [pickle.dumps(f'baz-{i}', protocol=4) for i in range(num_msgs)]  # type: List[Optional[bytes]]
        fake_data += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.data.side_effect = fake_data

        fake_ids = [i * 10 for i in range(num_msgs)]  # type: List[Optional[int]]
        fake_ids += [None]  # signifies end of stream -- not actually a message
        mock_con.return_value.subscribe.return_value.receive.return_value.message_id.side_effect = fake_ids

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # propagate_error=False
            for msg in gen:
                logging.debug(msg)
                raise TestException

        mock_con.return_value.close.assert_not_called()
        mock_con.return_value.subscribe.return_value.negative_acknowledge.assert_called_with(0)

        logging.info("Round 2")

        with q.recv() as gen:  # propagate_error=False
            mock_con.return_value.subscribe.return_value.acknowledge.assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            mock_con.return_value.subscribe.return_value.acknowledge.assert_called_with(last_id)

        mock_con.return_value.close.assert_called()
