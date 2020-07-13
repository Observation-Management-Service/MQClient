"""Define an interface that backends will adhere to."""

from typing import Generator, Optional, Union

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

# -------------------
# classes to override
# -------------------


class RawQueue:
    """Raw queue object, to hold queue state."""

    def close(self) -> None:
        """Close interface to queue."""
        raise NotImplementedError()

# ---------------------------------------
# Functions to be implemented in backends
# ---------------------------------------


def create_pub_queue(address: str, name: str) -> RawQueue:
    """Create a publishing queue."""
    raise NotImplementedError()


def create_sub_queue(address: str, name: str, prefetch: int = 1) -> RawQueue:
    """Create a subscription queue."""
    raise NotImplementedError()


def send_message(queue: RawQueue, msg: bytes) -> None:
    """Send a message on a queue."""
    raise NotImplementedError()


def get_message(queue: RawQueue) -> Optional[Message]:
    """Get a single message from a queue."""
    raise NotImplementedError()


def ack_message(queue: RawQueue, msg_id: MessageID) -> None:
    """Ack a message from the queue."""
    raise NotImplementedError()


def reject_message(queue: RawQueue, msg_id: MessageID) -> None:
    """Reject (nack) a message from the queue."""
    raise NotImplementedError()


def message_generator(queue: RawQueue, timeout: int = 60, auto_ack: bool = True,
                      propagate_error: bool = True) -> Generator[Optional[Message], None, None]:
    """Yield a Message.

    Args:
        queue (RabbitMQSub): queue object
        timeout (int): timeout in seconds for inactivity
        auto_ack (bool): Ack each message after successful processing
        propagate_error (bool): should errors from downstream code kill the generator?
    """
    raise NotImplementedError()
