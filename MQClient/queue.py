"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import logging
import types
import uuid
from typing import Any, Generator, Optional, Type

from .backend_interface import Backend, Message, Pub, Sub


class Queue:
    """User-facing queue library.

    Args:
        backend (Backend): the backend to use
        address (str): address of queue (default: 'localhost')
        name (str): name of queue (default: <random string>)
        prefetch (int): size of prefetch buffer for receiving messages (default: 1)
    """

    def __init__(
        self,
        backend: Backend,
        address: str = "localhost",
        name: str = "",
        prefetch: int = 1,
    ) -> None:
        self._backend = backend
        self._address = address
        self._name = name if name else Queue.make_name()
        self._prefetch = prefetch
        self._pub_queue: Optional[Pub] = None

    @staticmethod
    def make_name() -> str:
        """Return a pseudo-unique string that is a legal queue identifier.

        This name is valid for any backend chosen.
        """
        return "a" + (uuid.uuid4().hex)[:20]

    @property
    def backend(self) -> Backend:
        """Get backend instance responsible for managing queuing service."""
        return self._backend

    @property
    def address(self) -> str:
        """Get address of the queuing daemon."""
        return self._address

    @property
    def name(self) -> str:
        """Get name of queue."""
        return self._name

    @property
    def prefetch(self) -> int:
        """Get size of prefetch buffer for receiving messages."""
        return self._prefetch

    @prefetch.setter
    def prefetch(self, val: int) -> None:
        if val < 1:
            raise Exception("prefetch must be positive")
        if self._prefetch != val:
            self._prefetch = val

    @property
    def raw_pub_queue(self) -> Pub:
        """Get publisher queue."""
        if not self._pub_queue:
            self._pub_queue = self._backend.create_pub_queue(self._address, self._name)

        if not self._pub_queue:
            raise Exception("Pub queue failed to be created.")
        return self._pub_queue

    @raw_pub_queue.deleter
    def raw_pub_queue(self) -> None:
        logging.debug("Deleter Queue.raw_pub_queue")
        self._close_pub_queue()

    def _close_pub_queue(self) -> None:
        if self._pub_queue:
            logging.debug("Closing Queue._pub_queue")
            self._pub_queue.close()
            self._pub_queue = None

    def _create_sub_queue(self) -> Sub:
        """Wrap `self._backend.create_sub_queue()` with instance's config."""
        return self._backend.create_sub_queue(self._address, self._name, self._prefetch)

    def close(self) -> None:
        """Close all connections."""
        self._close_pub_queue()

    def send(self, data: Any) -> None:
        """Send a message to the queue.

        Arguments:
            data (Any): object of data to send (must be picklable)
        """
        self.raw_pub_queue.send_message(Message.serialize_data(data))

    def recv(
        self, timeout: int = 60, suppress_ctx_errors: bool = True
    ) -> "MessageGeneratorContext":
        """Receive a stream of messages from the queue.

        This returns a context-manager/generator. Its iterator stops when no
        messages are received for `timeout` seconds. If an exception is raised
        (inside the context), the message is rejected and messages can continue
        to be received (configured by `suppress_ctx_errors`). Multiple calls to
        `recv()` is okay.

        Example:
            with queue.recv() as stream:
                for data in stream:
                    ...

        Keyword Arguments:
            timeout -- seconds to wait for a message to be delivered
            suppress_ctx_errors -- whether to suppress interior context errors to the consumer
                    (when `True`, the context manager will also act like a `try-except` block)

        Returns:
            MessageGeneratorContext -- context manager and generator object
        """
        logging.debug("Creating new MessageGeneratorContext instance.")
        return MessageGeneratorContext(
            sub=self._create_sub_queue(),
            timeout=timeout,
            propagate_error=(not suppress_ctx_errors),
        )

    @contextlib.contextmanager
    def recv_one(self) -> Generator[Any, None, None]:
        """Receive one message from the queue.

        This is a context manager. If an exception is raised, the message is rejected.

        Decorators:
            contextlib.contextmanager

        Yields:
            Any -- object of data received, or None if queue is empty

        Raises:
            Exception -- [description]
        """
        sub = self._create_sub_queue()
        msg = sub.get_message()

        if not msg:
            raise Exception("No message available")

        try:
            yield msg.deserialize_data()
        except Exception:
            sub.reject_message(msg.msg_id)
            # TODO - check for self._suppress_ctx_errors
            raise
        else:
            sub.ack_message(msg.msg_id)
        finally:
            sub.close()

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return (
            f"Queue("
            f"{self.backend.__class__.__name__}, "
            f"address={self.address}, "
            f"name={self.name}, "
            f"prefetch={self.prefetch}, "
            f"pub={bool(self._pub_queue)}"
            f")"
        )


class MessageGeneratorContext:
    """A context manager wrapping `Sub.message_generator()`."""

    RUNTIME_ERROR_CONTEXT_STRING = (
        "'MessageGeneratorContext' object's runtime "
        "context has not been entered. Use 'with as' syntax."
    )

    def __init__(self, sub: Sub, timeout: int, propagate_error: bool) -> None:
        logging.debug("[MessageGeneratorContext.__init__()]")
        self.sub = sub
        self.message_generator = sub.message_generator(
            timeout=timeout, propagate_error=propagate_error
        )
        self.entered = False

    def __enter__(self) -> "MessageGeneratorContext":
        """Return instance.

        Triggered by 'with ... as'.
        """
        logging.debug("[MessageGeneratorContext.__enter__()] entered `with-as` block")
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> bool:
        """Return `True` to suppress any Exception raised by consumer code.

        Return `False` to re-raise/propagate that Exception.

        Arguments:
            exc_type {Optional[BaseException]} -- Exception type.
            exc_val {Optional[Type[BaseException]]} -- Exception object.
            exc_tb {Optional[types.TracebackType]} -- Exception Traceback.
        """
        logging.debug(
            f"[MessageGeneratorContext.__exit__()] exiting `with-as` block (exc:{exc_type})"
        )
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        self.sub.close()

        # Exception was raised
        if exc_type and exc_val:
            try:
                self.message_generator.throw(exc_type, exc_val, exc_tb)
            except exc_type:  # message_generator re-raised Exception
                return False  # don't suppress the Exception
        return True  # suppress any Exception

    def __iter__(self) -> "MessageGeneratorContext":
        """Return instance.

        Triggered with 'for'/'iter()'.
        """
        logging.debug("[MessageGeneratorContext.__iter__()] entered loop/`iter()`")
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)
        return self

    def __next__(self) -> Any:
        """Return next Message in queue."""
        logging.debug("[MessageGeneratorContext.__next__()] next iteration...")
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        try:
            msg = next(self.message_generator)
        except StopIteration:
            logging.debug(
                "[MessageGeneratorContext.__next__()] end of loop (StopIteration)"
            )
            raise
        if not msg:
            raise RuntimeError(
                "Yielded value is `None`. This should not have happened."
            )

        return msg.deserialize_data()
