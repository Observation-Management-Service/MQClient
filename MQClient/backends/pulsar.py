"""Back-end using Apache Pulsar."""
import typing

import pulsar  # type: ignore

from ..backend_interface import Message, MessageID, RawQueue

# Private Classes


class Pulsar(RawQueue):
    """Base Pulsar wrapper.

    Extends:
        RawQueue
    """

    def __init__(self, address: str, queue: str) -> None:
        self.address = address
        if not self.address.startswith('pulsar'):
            self.address = 'pulsar://' + self.address
        self.queue = queue  # topic
        self.client = None  # type: pulsar.Client

    def connect(self):
        """Set up client."""
        self.client = pulsar.Client(self.address)


class PulsarPub(Pulsar):
    """Wrapper around pulsar.Producer.

    Extends:
        Pulsar
    """

    def __init__(self, address: str, queue: str) -> None:
        super().__init__(address, queue)
        self.producer = None  # type: pulsar.Producer

    def connect(self):
        """Connect to producer."""
        super().connect()
        self.producer = self.client.create_producer(self.queue)


class PulsarSub(Pulsar):
    """Wrapper around pulsar.Consumer.

    Extends:
        Pulsar
    """

    def __init__(self, address: str, queue: str) -> None:
        super().__init__(address, queue)
        self.consumer = None  # type: pulsar.Consumer
        self.subscription_name = f'{self.queue}-subscription'  # single shared subscription
        self.prefetch = 1

    def connect(self):
        """Connect to subscriber."""
        super().connect()
        self.consumer = self.client.subscribe(self.queue,
                                              self.subscription_name,
                                              receiver_queue_size=self.prefetch)

# Interface Methods


def create_pub_queue(address: str, queue: str) -> PulsarPub:
    """Create a publishing queue."""
    q = PulsarPub(address, queue)
    q.connect()
    return q


def create_sub_queue(address: str, queue: str, prefetch: int = 1) -> PulsarSub:
    """Create a subscription queue."""
    q = PulsarSub(address, queue)
    q.prefetch = prefetch
    q.connect()
    return q


def send_message(queue: PulsarPub, msg: bytes) -> None:
    """Send a message on a queue."""
    if not queue.producer:
        raise RuntimeError("queue is not connected")

    queue.producer.send(msg)


def get_message(queue: PulsarSub) -> typing.Optional[Message]:
    """Get a single message from a queue."""
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    msg = queue.consumer.receive()
    return Message(msg.message_id(), msg.data())


def ack_message(queue: PulsarSub, msg_id: MessageID) -> None:
    """Ack a message from the queue."""
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    queue.consumer.acknowledge(msg_id)


def reject_message(queue: PulsarSub, msg_id: MessageID) -> None:
    """Reject (nack) a message from the queue."""
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    queue.consumer.negative_acknowledge(msg_id)


def message_generator(queue: PulsarSub, timeout: int, auto_ack: bool = True,
                      propagate_error: bool = True) -> None:
    """
    Yield Messages.

    Args:
        queue (PulsarSub): queue object
        timeout (int): timeout in seconds for inactivity
        auto_ack (bool): Ack each message after successful processing
        propagate_error (bool): should errors from downstream code kill the generator?
    """
    if not queue.consumer:
        raise RuntimeError("queue is not connected")
