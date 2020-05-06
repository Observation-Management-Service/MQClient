"""Back-end using Apache Pulsar."""
import typing

from ..backend_interface import Message, MessageID, RawQueue

# Private Classes


class Pulsar(RawQueue):
    pass


# Interface Methods

def create_pub_queue() -> RawQueue:
    """Create a publishing queue"""
    raise NotImplementedError()


def create_sub_queue() -> RawQueue:
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
