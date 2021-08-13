"""Define an interface that backends will adhere to."""


import pickle
from enum import Enum, auto
from typing import Any, Dict, Generator, Optional, Union

MessageID = Union[int, str, bytes]

TIMEOUT_MILLIS_DEFAULT = 1000  # milliseconds
RETRY_DELAY = 1  # seconds
TRY_ATTEMPTS = 3  # ex: 3 means 1 initial try and 2 retries


class ClosingFailedExcpetion(Exception):
    """Raised when a `close()` invocation fails."""


class AlreadyClosedExcpetion(ClosingFailedExcpetion):
    """Raised when a `close()` invocation fails on an already closed interface."""


class AckException(Exception):
    """Raised when there's a problem with acking."""


class NackException(Exception):
    """Raised when there's a problem with nacking."""


class Message:
    """Message object.

    Holds msg_id and data.
    """

    class AckStatus(Enum):
        """Signify the ack state of a message."""

        NONE = auto()  # message has not been acked nor nacked
        ACKED = auto()  # message has been acked
        NACKED = auto()  # message has been nacked

    def __init__(self, msg_id: MessageID, payload: bytes):
        if not isinstance(msg_id, (int, str, bytes)):
            raise TypeError(
                f"Message.msg_id must be type int|str|bytes (not '{type(msg_id)}')."
            )
        if not isinstance(payload, bytes):
            raise TypeError(
                f"Message.data must be type 'bytes' (not '{type(payload)}')."
            )
        self.msg_id = msg_id
        self.payload = payload
        self.ack_status: Message.AckStatus = Message.AckStatus.NONE

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return f"Message(msg_id={self.msg_id!r}, payload={self.payload!r})"

    def __eq__(self, other: object) -> bool:
        """Return True if self's and other's `data` are equal.

        On redelivery, `msg_id` may differ from its original, so
        `msg_id` is not a reliable source for testing equality. And
        neither is the `headers` field.
        """
        return (
            bool(other)
            and isinstance(other, Message)
            and self.deserialize_data() == other.deserialize_data()
        )

    def deserialize_data(self) -> Any:
        """Read and return an object from the `data` field."""
        return pickle.loads(self.payload)["data"]

    @staticmethod  # TODO: rename to just `serialize()`
    def serialize_data(data: Any, headers: Optional[Dict[str, Any]] = None) -> bytes:
        """Return serialized representation of message payload as a bytes object.

        Optionally include `headers` dict for internal information.
        """
        return pickle.dumps({"headers": headers, "data": data}, protocol=4)


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
        self, timeout_millis: Optional[int] = TIMEOUT_MILLIS_DEFAULT
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

        Generate messages with variable timeout.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
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
