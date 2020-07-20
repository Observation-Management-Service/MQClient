"""Parent class for backend unit tests."""

import logging
import uuid
from typing import Any

import pytest  # type: ignore

# local imports
from MQClient.backend_interface import Backend

logging.getLogger().setLevel(logging.DEBUG)


class BackendUnitTest:
    """Unit test suite interface for specified backend."""

    backend = None  # type: Backend
    con_patch = ''

    @pytest.fixture  # type: ignore
    def mock_con(self, mocker: Any) -> Any:
        """Patch mock_con."""
        return mocker.patch(self.con_patch)

    @staticmethod
    @pytest.fixture  # type: ignore
    def queue_name() -> str:
        """Get random queue name."""
        name = uuid.uuid4().hex
        logging.info(f"NAME :: {name}")
        return name

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
        raise NotImplementedError()

    def test_reject_message(self, mock_con: Any, queue_name: str) -> None:
        """Test rejecting message."""
        raise NotImplementedError()

    def test_message_generator_0(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        raise NotImplementedError()

    def test_message_generator_1(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        raise NotImplementedError()

    def test_message_generator_2(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator."""
        raise NotImplementedError()

    def test_message_generator_upstream_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception originating upstream (a.k.a.
        from pulsar-package code).
        """
        raise NotImplementedError()

    def test_message_generator_no_auto_ack(self, mock_con: Any, queue_name: str) -> None:
        """Test message generator.

        Generator should not ack messages.
        """
        raise NotImplementedError()

    def test_message_generator_propagate_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should raise Exception, nack, and close. Unlike in an
        integration test, nacked messages are not put back on the queue.
        """
        raise NotImplementedError()

    def test_message_generator_suppress_error(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Generator should not raise Exception. Unlike in an integration
        test, nacked messages are not put back on the queue.
        """
        raise NotImplementedError()

    def test_message_generator_consumer_exception_0(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test message generator.

        Not so much a test, as an example of why MessageGeneratorContext
        is needed.
        """
        raise NotImplementedError()

    def test_queue_recv_consumer(self, mock_con: Any, queue_name: str) -> None:
        """Test Queue.recv()."""
        raise NotImplementedError()

    def test_queue_recv_comsumer_exception_0(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        When an Exception is raised in `with` block, the Queue should:
        - NOT close (pub) on exit
        - nack the message
        - suppress the Exception
        """
        raise NotImplementedError()

    def test_queue_recv_comsumer_exception_1(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        The Queue.recv()'s context manager should be reusable after
        suppressing an Exception.
        """
        raise NotImplementedError()

    def test_queue_recv_comsumer_exception_2(self, mock_con: Any, queue_name: str) -> None:
        """Failure-test Queue.recv().

        Same as test_queue_recv_comsumer_exception_1() but with multiple
        recv() calls.
        """
        raise NotImplementedError()
