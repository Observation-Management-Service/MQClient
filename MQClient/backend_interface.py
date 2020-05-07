"""
Define an interface that backends will adhere to.
"""

import typing

MessageID = typing.Union[int, str, bytes]


class Message:
    """Message object. Holds msg_id and data."""

    def __init__(self, msg_id: MessageID, data: bytes):
        self.msg_id = msg_id
        self.data = data

# -------------------
# classes to override
# -------------------


class RawQueue:
    """Raw queue object, to hold queue state."""
    pass

# ---------------------------------------
# Functions to be implemented in backends
# ---------------------------------------


def create_pub_queue(address: str, topic: str) -> RawQueue:
    """Create a publishing queue"""
    raise NotImplementedError()


def create_sub_queue(address: str, topic: str, prefetch: int = 1) -> RawQueue:
    """Create a subscription queue"""
    raise NotImplementedError()


def send_message(queue: RawQueue, msg: bytes) -> None:
    """Send a message on a queue"""
    raise NotImplementedError()


def get_message(queue: RawQueue) -> typing.Optional[Message]:
    """Get a single message from a queue"""
    raise NotImplementedError()


def ack_message(queue: RawQueue, msg_id: MessageID) -> None:
    """Ack a message from the queue"""
    raise NotImplementedError()


def reject_message(queue: RawQueue, msg_id: MessageID) -> None:
    """Reject (nack) a message from the queue"""
    raise NotImplementedError()


def message_generator(queue: RawQueue, timeout: int, auto_ack: bool = True,
                      propagate_error: bool = True) -> None:
    """
    A generator yielding a Message.

    Args:
        queue (RabbitMQSub): queue object
        timeout (int): timeout in seconds for inactivity
        auto_ack (bool): Ack each message after successful processing
        propagate_error (bool): should errors from downstream code kill the generator?
    """
    raise NotImplementedError()
