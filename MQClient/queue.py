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
        backend: the backend to use
        address: address of queue
        name: name of queue
        prefetch: size of prefetch buffer for receiving messages
        timeout: seconds to wait for a message to be delivered
        except_errors: whether to suppress interior context errors to
                        the consumer (when `True`, the context manager
                        will also act like a `try-except` block)
        auto_ack: whether to automatically acknowledge the received
                    message after successful receipt
        nack_on_error: whether to automatically reject/nack the received
                    message unsuccessful processing, i.e. exception was
                    raised

    """

    def __init__(
        self,
        backend: Backend,
        address: str = "localhost",
        name: str = "",
        prefetch: int = 1,
        timeout: int = 60,
        except_errors: bool = True,
        auto_ack: bool = True,
        nack_on_error: bool = True,
    ) -> None:
        self._backend = backend
        self._address = address
        self._name = name if name else Queue.make_name()
        self._prefetch = prefetch
        self._pub_queue: Optional[Pub] = None

        # publics
        self._timeout = 0
        self.timeout = timeout
        self.except_errors = except_errors
        self.auto_ack = auto_ack
        self.nack_on_error = nack_on_error

    @staticmethod
    def make_name() -> str:
        """Return a pseudo-unique string that is a legal queue identifier.

        This name is valid for any backend chosen.
        """
        return "a" + (uuid.uuid4().hex)[:20]

    @property
    def timeout(self) -> int:
        """Get the timeout value."""
        return self._timeout

    @timeout.setter
    def timeout(self, val: int) -> None:
        if val < 1:
            raise Exception("prefetch must be positive")
        self._timeout = val

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
        """Close all persisted connections."""
        self._close_pub_queue()

    def send(self, data: Any) -> None:
        """Send a message to the queue.

        Arguments:
            data (Any): object of data to send (must be picklable)
        """
        self.raw_pub_queue.send_message(Message.serialize_data(data))

    def ack(self, sub: Sub, msg: Message) -> None:  # pylint:disable=no-self-use
        """Acknowledge the message."""
        # TODO - add guardrails
        sub.ack_message(msg)

    def nack(self, sub: Sub, msg: Message) -> None:  # pylint:disable=no-self-use
        """Reject/nack the message."""
        # TODO - add guardrails
        sub.reject_message(msg)

    def recv(self) -> "MessageGeneratorContext":
        """Receive a stream of messages from the queue.

        This returns a context-manager/generator. Its iterator stops when no
        messages are received for `timeout` seconds. If an exception is raised
        (inside the context), the message is rejected, the context is exited,
        and exception can be re-raised if configured by `except_errors`.
        Multiple calls to `recv()` is okay, but reusing the returned instance
        is not.

        Example:
            with queue.recv() as stream:
                for data in stream:
                    ...

        Returns:
            MessageGeneratorContext -- context manager and generator object
        """
        logging.debug("Creating new MessageGeneratorContext instance.")
        return MessageGeneratorContext(self._create_sub_queue(), self)

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
        msg = sub.get_message(self.timeout * 1000)

        if not msg:
            raise Exception("No message available")

        data = msg.deserialize_data()
        try:
            yield data
        except Exception:  # pylint:disable=broad-except
            if self.nack_on_error:
                self.nack(sub, msg)
            if not self.except_errors:
                raise
        else:
            if self.auto_ack:
                self.ack(sub, msg)
        finally:
            sub.close()

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return (
            f"Queue("
            f"{self._backend.__class__.__name__}, "
            f"address={self._address}, "
            f"name={self._name}, "
            f"prefetch={self._prefetch}, "
            f"pub={bool(self._pub_queue)}"
            f")"
        )


class MessageGeneratorContext:
    """A context manager wrapping `Sub.message_generator()`."""

    RUNTIME_ERROR_CONTEXT_STRING = (
        "'MessageGeneratorContext' object's runtime "
        "context has not been entered. Use 'with as' syntax."
    )

    def __init__(self, sub: Sub, queue: Queue) -> None:
        logging.debug("[MessageGeneratorContext.__init__()]")
        self.sub = sub
        self.message_generator = sub.message_generator(
            timeout=queue.timeout, propagate_error=(not queue.except_errors)
        )
        self.queue = queue

        self.entered = False
        self.msg: Optional[Message] = None

    def __enter__(self) -> "MessageGeneratorContext":
        """Return instance.

        Triggered by 'with ... as'.
        """
        logging.debug("[MessageGeneratorContext.__enter__()] entered `with-as` block")

        if self.entered:
            raise RuntimeError(
                "A 'MessageGeneratorContext' instance cannot be re-entered."
            )

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

        reraise_exception = False

        # Exception Was Raised
        if exc_type and exc_val:
            if self.msg and self.queue.nack_on_error:
                self.queue.nack(self.sub, self.msg)
            # see how the generator wants to handle the exception
            try:
                # `throw` is caught by the message_generator's try-except around `yield`
                self.message_generator.throw(exc_type, exc_val, exc_tb)
            except exc_type:  # message_generator re-raised Exception
                reraise_exception = True
        # Good Exit (No Original Exception)
        else:
            if self.queue.auto_ack and self.msg:
                self.queue.ack(self.sub, self.msg)

        self.sub.close()  # close after cleanup

        if reraise_exception:
            logging.debug(
                "[MessageGeneratorContext.__exit__()] exited & propagated error."
            )
            return False  # propagate the Exception!
        else:
            # either no exception or suppress the exception
            if exc_type and exc_val:
                logging.debug(
                    "[MessageGeneratorContext.__exit__()] exited & suppressed error."
                )
            else:
                logging.debug("[MessageGeneratorContext.__exit__()] exited w/o error.")
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

        # ack the previous message before getting a new one
        if self.queue.auto_ack and self.msg:
            self.queue.ack(self.sub, self.msg)

        try:
            self.msg = next(self.message_generator)
        except StopIteration:
            logging.debug(
                "[MessageGeneratorContext.__next__()] end of loop (StopIteration)"
            )
            raise
        if not self.msg:
            raise RuntimeError(
                "Yielded value is `None`. This should not have happened."
            )

        return self.msg.deserialize_data()
