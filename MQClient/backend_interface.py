"""Define an interface that backends will adhere to."""


import pickle
from enum import Enum, auto
from typing import Any, Generator, Optional, Union

MessageID = Union[int, str, bytes]

GET_MSG_TIMEOUT = 1000


class ClosingFailedExcpetion(Exception):
    """Raised when a `close()` invocation fails."""


class AlreadyClosedExcpetion(ClosingFailedExcpetion):
    """Raised when a `close()` invocation fails on an already closed interface."""


class Message:
    """Message object.

    Holds msg_id and data.
    """

    class AckStatus(Enum):
        """Signify the ack state of a message."""

        NONE = auto()  # message has not been acked nor nacked
        ACKED = auto()  # message has been acked
        NACKED = auto()  # message has been nacked

    def __init__(self, msg_id: MessageID, data: bytes):
        if not isinstance(msg_id, (int, str, bytes)):
            raise TypeError(
                f"Message.msg_id must be type int|str|bytes (not '{type(msg_id)}')."
            )
        if not isinstance(data, bytes):
            raise TypeError(f"Message.data must be type 'bytes' (not '{type(data)}').")
        self.msg_id = msg_id
        self.data = data
        self.ack_status: Message.AckStatus = Message.AckStatus.NONE

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return f"Message(msg_id={self.msg_id!r}, data={self.data!r})"

    def __eq__(self, other: object) -> bool:
        """Return True if self's and other's `data` are equal.

        On redelivery, `msg_id` may differ from its original, so
        `msg_id` is not a reliable source for testing equality.
        """
        return bool(other) and isinstance(other, Message) and (self.data == other.data)

    def deserialize_data(self) -> Any:
        """Read and return an object from `data` (bytes)."""
        return pickle.loads(self.data)

    @staticmethod
    def serialize_data(data: Any) -> bytes:
        """Return serialized representation of `data` as a bytes object."""
        return pickle.dumps(data, protocol=4)


# -----------------------------
# classes to override/implement
# -----------------------------


class RawQueue:
    """Raw queue object, to hold queue state."""

    def __init__(self) -> None:
        pass

    def connect(self) -> None:
        """Set up connection."""

    def close(self) -> None:
        """Close interface to queue."""


class Pub(RawQueue):
    """Publisher queue."""

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue."""
        raise NotImplementedError()


class Sub(RawQueue):
    """Subscriber queue."""

    @staticmethod
    def _to_message(*args: Any) -> Optional[Message]:
        """Convert backend-specific payload to standardized Message type."""
        raise NotImplementedError()

    def get_message(
        self, timeout_millis: Optional[int] = GET_MSG_TIMEOUT
    ) -> Optional[Message]:
        """Get a single message from a queue."""
        raise NotImplementedError()

    def ack_message(self, msg: Message) -> None:
        """Ack a message from the queue."""
        raise NotImplementedError()

    def reject_message(self, msg: Message) -> None:
        """Reject (nack) a message from the queue."""
        raise NotImplementedError()

    def message_generator(
        self, timeout: int = 60, propagate_error: bool = True
    ) -> Generator[Optional[Message], None, None]:
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
