"""Unit Tests for RabbitMQ/Pika Backend."""

import logging
import pickle
import unittest
from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest  # type: ignore

# local imports
from MQClient import Queue
from MQClient.backends import rabbitmq

from .common_unit_tests import BackendUnitTest


class TestUnitRabbitMQ(BackendUnitTest):
    """Unit test suite interface for RabbitMQ backend."""

    backend = rabbitmq.Backend()
    con_patch = 'pika.BlockingConnection'

    def test_create_pub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating pub queue."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        assert q.queue == queue_name
        mock_con.return_value.channel.assert_called()

    def test_create_sub_queue(self, mock_con: Any, queue_name: str) -> None:
        """Test creating sub queue."""
        q = self.backend.create_sub_queue("localhost", queue_name, prefetch=213)
        assert q.queue == queue_name
        assert q.prefetch == 213
        mock_con.return_value.channel.assert_called()

    def test_send_message(self, mock_con: Any, queue_name: str) -> None:
        """Test sending message."""
        q = self.backend.create_pub_queue("localhost", queue_name)
        q.send_message(b"foo, bar, baz")
        mock_con.return_value.channel.return_value.basic_publish.assert_called_with(
            exchange='',
            routing_key=queue_name,
            body=b'foo, bar, baz',
        )

    def test_get_message(self, mock_con: Any, queue_name: str) -> None:
        """Test getting message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
        mock_con.return_value.channel.return_value.basic_get.return_value = fake_message
        m = q.get_message()
        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'

    def test_ack_message(self, mock_con: Any, queue_name: str) -> None:
        """Test acking message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        q.ack_message(12)
        mock_con.return_value.channel.return_value.basic_ack.assert_called_with(12)

    def test_reject_message(self, mock_con: Any, queue_name: str) -> None:
        """Test rejecting message."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        q.reject_message(12)
        mock_con.return_value.channel.return_value.basic_nack.assert_called_with(12)

    def test_message_generator_0(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        num_msgs = 100

        fake_messages = [(MagicMock(delivery_tag=i * 10), None, f'baz-{i}'.encode('utf-8')) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
        fake_messages += [(None, None, None)]  # signifies end of stream -- not actually a message
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        for i, msg in enumerate(q.message_generator()):
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                prev_id = (i - 1) * 10
                mock_con.return_value.channel.return_value.basic_ack.assert_called_with(prev_id)
            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.data == fake_messages[i][2]
        mock_con.return_value.channel.return_value.basic_ack.assert_called_with((num_msgs - 1) * 10)
        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_message_generator_1(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
        fake_message2 = (MagicMock(delivery_tag=20), None, b'baz')
        mock_con.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
        m = None
        for i, x in enumerate(q.message_generator()):
            m = x
            if i == 0:
                break

        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'
        mock_con.return_value.channel.return_value.basic_ack.assert_called_with(12)
        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_message_generator_2(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        q = self.backend.create_sub_queue("localhost", queue_name)
        fake_message = (MagicMock(delivery_tag=12), None, b'foo, bar')
        fake_message2 = (None, None, None)  # signifies end of stream -- not actually a message
        mock_con.return_value.channel.return_value.consume.return_value = [fake_message, fake_message2]
        m = None
        for i, x in enumerate(q.message_generator()):
            assert i < 1
            m = x
        assert m is not None
        assert m.msg_id == 12
        assert m.data == b'foo, bar'
        mock_con.return_value.channel.return_value.basic_ack.assert_called_with(12)
        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_message_generator_upstream_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pika-package code).
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        err_msg = (unittest.mock.ANY, None, b'foo, bar')
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = list(q.message_generator())
        mock_con.return_value.channel.return_value.cancel.assert_called()

        # `propagate_error` attribute has no affect (b/c it deals w/ *downstream* errors)
        err_msg = (unittest.mock.ANY, None, b'foo, bar')
        mock_con.return_value.channel.return_value.consume.return_value = [err_msg]
        with pytest.raises(Exception):
            _ = list(q.message_generator(propagate_error=False))
        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_message_generator_no_auto_ack(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator.

        Generator should not ack messages.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        fake_messages = [
            (MagicMock(delivery_tag=0), None, b'baz-0'),
            (MagicMock(delivery_tag=1), None, b'baz-1'),
            (MagicMock(delivery_tag=2), None, b'baz-2'),
            (None, None, None)  # signifies end of stream -- not actually a message
        ]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        gen = q.message_generator(auto_ack=False)
        i = 0
        for msg in gen:
            logging.debug(i)
            if i > 0:  # see if previous msg was acked
                mock_con.return_value.channel.return_value.basic_ack.assert_not_called()

            assert msg is not None
            assert msg.msg_id == i
            assert msg.data == fake_messages[i][2]

            i += 1

    def test_message_generator_propagate_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception, nack, and close. Unlike in an
        integration test, nacked messages are not put back on the queue.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        fake_messages = [
            (MagicMock(delivery_tag=0), None, b'baz-0'),
            (MagicMock(delivery_tag=1), None, b'baz-1'),
            (MagicMock(delivery_tag=2), None, b'baz-2')
        ]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        gen = q.message_generator()  # propagate_error=True
        i = 0
        for msg in gen:
            logging.debug(i)
            assert i < 3
            if i > 0:  # see if previous msg was acked
                mock_con.return_value.channel.return_value.basic_ack.assert_called_with(i - 1)

            assert msg is not None
            assert msg.msg_id == i
            assert msg.data == fake_messages[i][2]

            if i == 2:
                with pytest.raises(Exception):
                    gen.throw(Exception)
                mock_con.return_value.channel.return_value.basic_nack.assert_called_with(i)
                mock_con.return_value.channel.return_value.cancel.assert_called()

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

        fake_messages = [(MagicMock(delivery_tag=i * 10), None, f'baz-{i}'.encode('utf-8')) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
        fake_messages += [(None, None, None)]  # signifies end of stream -- not actually a message
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        gen = q.message_generator(propagate_error=False)
        i = 0
        # odds are acked and evens are nacked
        for msg in gen:
            logging.debug(i)
            if i > 0:
                prev_id = (i - 1) * 10
                if i % 2 == 0:  # see if previous EVEN msg was acked
                    mock_con.return_value.channel.return_value.basic_ack.assert_called_with(prev_id)
                else:  # see if previous ODD msg was NOT acked
                    with pytest.raises(AssertionError):
                        mock_con.return_value.channel.return_value.basic_ack.assert_called_with(prev_id)

            assert msg is not None
            assert msg.msg_id == i * 10
            assert msg.data == fake_messages[i][2]

            if i % 2 == 0:
                gen.throw(Exception)
                mock_con.return_value.channel.return_value.basic_nack.assert_called_with(i * 10)

            i += 1
        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_message_generator_consumer_exception_fail(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Not so much a test, as an example of why MessageGeneratorContext
        is needed.
        """
        q = self.backend.create_sub_queue("localhost", queue_name)

        fake_message = (MagicMock(delivery_tag=0), None, b'baz')
        mock_con.return_value.channel.return_value.consume.return_value = fake_message

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
            mock_con.return_value.channel.return_value.cancel.assert_not_called()
        with pytest.raises(AssertionError):
            mock_con.return_value.channel.return_value.basic_nack.assert_called_with(0)

    def test_queue_recv_consumer(self, mock_con: Any, queue_name: str) -> None:
        """Test Queue.recv()."""
        q = Queue(self.backend, address="localhost", name=queue_name)

        fake_messages = [
            (MagicMock(delivery_tag=0), None, pickle.dumps('baz', protocol=4)),
            (None, None, None)  # signifies end of stream -- not actually a message
        ]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        with q.recv() as gen:
            for msg in gen:
                logging.debug(msg)
                assert msg
                assert msg == pickle.loads(fake_messages[0][2])  # type: ignore

        mock_con.return_value.channel.return_value.cancel.assert_called()
        mock_con.return_value.channel.return_value.basic_ack.assert_called_with(0)

    def test_queue_recv_comsumer_exception_0(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        When an Exception is raised in `with` block, the Queue should:
        - NOT close (pub) on exit
        - nack the message
        - suppress the Exception
        """
        q = Queue(self.backend, address="localhost", name=queue_name)

        fake_messages = [
            (MagicMock(delivery_tag=0), None, pickle.dumps('baz-0', protocol=4)),
            (MagicMock(delivery_tag=1), None, pickle.dumps('baz-1', protocol=4))
        ]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # propagate_error=False
            for i, msg in enumerate(gen):
                assert i == 0
                logging.debug(msg)
                raise TestException

        mock_con.return_value.channel.return_value.cancel.assert_not_called()
        mock_con.return_value.channel.return_value.basic_nack.assert_called_with(0)

    def test_queue_recv_comsumer_exception_1(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        The Queue.recv()'s context manager should be reusable after
        suppressing an Exception.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        num_msgs = 12

        fake_messages = [(MagicMock(delivery_tag=i * 10), None, pickle.dumps(f'baz-{i}', protocol=4)) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
        fake_messages += [(None, None, None)]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        class TestException(Exception):  # pylint: disable=C0115
            pass

        g = q.recv()
        with g as gen:  # propagate_error=False
            for msg in gen:
                logging.debug(msg)
                raise TestException

        mock_con.return_value.channel.return_value.cancel.assert_not_called()
        mock_con.return_value.channel.return_value.basic_nack.assert_called_with(0)

        logging.info("Round 2")

        with g as gen:  # propagate_error=False
            mock_con.return_value.channel.return_value.basic_ack.assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    mock_con.return_value.channel.return_value.basic_ack.assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            mock_con.return_value.channel.return_value.basic_ack.assert_called_with(last_id)

        mock_con.return_value.channel.return_value.cancel.assert_called()

    def test_queue_recv_comsumer_exception_2(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        Same as test_queue_recv_comsumer_exception_1() but with multiple
        recv() calls.
        """
        q = Queue(self.backend, address="localhost", name=queue_name)
        num_msgs = 12

        fake_messages = [(MagicMock(delivery_tag=i * 10), None, pickle.dumps(f'baz-{i}', protocol=4)) for i in range(num_msgs)]  # type: List[Tuple[Optional[MagicMock], None, Optional[bytes]]]
        fake_messages += [(None, None, None)]
        mock_con.return_value.channel.return_value.consume.return_value = fake_messages

        class TestException(Exception):  # pylint: disable=C0115
            pass

        with q.recv() as gen:  # propagate_error=False
            for msg in gen:
                logging.debug(msg)
                raise TestException

        mock_con.return_value.channel.return_value.cancel.assert_not_called()
        mock_con.return_value.channel.return_value.basic_nack.assert_called_with(0)

        logging.info("Round 2")

        with q.recv() as gen:  # propagate_error=False
            mock_con.return_value.channel.return_value.basic_ack.assert_not_called()
            for i, msg in enumerate(gen, start=1):
                logging.debug(f"{i} :: {msg}")
                if i > 1:  # see if previous msg was acked
                    prev_id = (i - 1) * 10
                    mock_con.return_value.channel.return_value.basic_ack.assert_called_with(prev_id)

            last_id = (num_msgs - 1) * 10
            mock_con.return_value.channel.return_value.basic_ack.assert_called_with(last_id)

        mock_con.return_value.channel.return_value.cancel.assert_called()
