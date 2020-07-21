"""Define an interface that backends will adhere to."""

import logging
import pickle
import types
from typing import Any, Generator, Optional, Type, Union

MessageID = Union[int, str, bytes]


class Message:
    """Message object.

    Holds msg_id and data.
    """

    def __init__(self, msg_id: MessageID, data: bytes):
        if not isinstance(msg_id, (int, str, bytes)):
            raise TypeError(f"Message.msg_id must be type 'int', 'str', or 'bytes' (not '{type(msg_id)}').")
        if not isinstance(data, bytes):
            raise TypeError(f"Message.data must be type 'bytes' (not '{type(data)}').")
        self.msg_id = msg_id
        self.data = data

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return f"Message(msg_id={self.msg_id!r}, data={self.data!r})"

    def __eq__(self, other: object) -> bool:
        """Return True if self's and other's `data` are equal.

        On redelivery, `msg_id` may differ from its original, so
        `msg_id` is not a reliable source for testing equality.
        """
        return bool(other) and isinstance(other, Message) and (self.data == other.data)


# -----------------------------
# classes to override/implement
# -----------------------------


class RawQueue:
    """Raw queue object, to hold queue state."""

    def __init__(self) -> None:
        self.was_closed = False

    def connect(self) -> None:
        """Set up connection."""
        self.was_closed = False

    def close(self) -> None:
        """Close interface to queue."""
        self.was_closed = True


class Pub(RawQueue):
    """Publisher queue."""

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue."""
        raise NotImplementedError()


class Sub(RawQueue):
    """Subscriber queue."""

    def get_message(self) -> Optional[Message]:
        """Get a single message from a queue."""
        raise NotImplementedError()

    def ack_message(self, msg_id: MessageID) -> None:
        """Ack a message from the queue."""
        raise NotImplementedError()

    def reject_message(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue."""
        raise NotImplementedError()

    def message_generator(self, timeout: int = 60, auto_ack: bool = True,
                          propagate_error: bool = True) -> Generator[Optional[Message], None, None]:
        """Yield Messages.

        Generate messages with variable timeout. Close instance on exit and error.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            auto_ack {bool} -- Ack each message after successful processing (default: {True})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        raise NotImplementedError()


class Backend:
    """Backend Pub-Sub Factory."""

    @staticmethod
    def create_pub_queue(address: str, name: str) -> Pub:
        """Create a publishing queue."""
        raise NotImplementedError()

    @staticmethod
    def create_sub_queue(address: str, name: str, prefetch: int = 1) -> Sub:
        """Create a subscription queue."""
        raise NotImplementedError()


# --------------------------------------------------------------------------------
# classes to interface between Queue and backend_interface's (implemented) classes
# --------------------------------------------------------------------------------


class MessageGeneratorContext:
    """A context manager wrapping backend.message_generator()."""

    RUNTIME_ERROR_CONTEXT_STRING = "'MessageGeneratorContext' object's runtime context has not been entered. Use 'with as' syntax."

    def __init__(self, sub: Sub, timeout: int, propagate_error: bool) -> None:
        logging.debug("in __init__")
        self.message_generator = sub.message_generator(timeout=timeout,
                                                       propagate_error=propagate_error)
        self.entered = False

    def __enter__(self) -> 'MessageGeneratorContext':
        """Return instance.

        Triggered by 'with ... as'.
        """
        logging.debug("in __enter__")
        self.entered = True
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[types.TracebackType]) -> bool:
        """Return `True` to suppress any Exception raised by consumer code.

        Return `False` to re-raise/propagate that Exception.

        Arguments:
            exc_type {Optional[BaseException]} -- Exception type.
            exc_val {Optional[Type[BaseException]]} -- Exception object.
            exc_tb {Optional[types.TracebackType]} -- Exception Traceback.
        """
        logging.debug(f"in __exit__: {exc_type}")
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        # Exception was raised
        if exc_type and exc_val:
            try:
                self.message_generator.throw(exc_type, exc_val, exc_tb)
            except exc_type:  # message_generator re-raised Exception
                return False  # don't suppress the Exception
        return True  # suppress any Exception

    def __iter__(self) -> 'MessageGeneratorContext':
        """Return instance.

        Triggered with 'for'/'iter()'.
        """
        logging.debug("in __iter__")
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)
        return self

    def __next__(self) -> Any:
        """Return next Message in queue."""
        logging.debug("in __next__")
        if not self.entered:
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        try:
            msg = next(self.message_generator)
        except StopIteration:
            logging.debug("StopIteration")
            raise
        if not msg:
            raise RuntimeError("Yielded value is `None`. This should not have happened.")

        data = pickle.loads(msg.data)
        return data
