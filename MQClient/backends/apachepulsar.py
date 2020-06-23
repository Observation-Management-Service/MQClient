"""Back-end using Apache Pulsar."""

import logging
import time
import typing

import pulsar  # type: ignore

from ..backend_interface import Message, MessageID, RawQueue
from . import log_msgs

# Private Classes


class Pulsar(RawQueue):
    """Base Pulsar wrapper.

    Extends:
        RawQueue
    """

    def __init__(self, address: str, topic: str) -> None:
        self.address = address
        if not self.address.startswith('pulsar'):
            self.address = 'pulsar://' + self.address
        self.topic = topic
        self.client = None  # type: pulsar.Client

    def connect(self) -> None:
        """Set up client."""
        self.client = pulsar.Client(self.address)

    def close(self) -> None:
        """Close client."""
        if self.client:
            try:
                self.client.close()
            except Exception as e:  # pylint: disable=W0703
                if str(e) != "Pulsar error: AlreadyClosed":
                    raise


class PulsarPub(Pulsar):
    """Wrapper around pulsar.Producer.

    Extends:
        Pulsar
    """

    def __init__(self, address: str, topic: str) -> None:
        super().__init__(address, topic)
        self.producer = None  # type: pulsar.Producer

    def connect(self) -> None:
        """Connect to producer."""
        super().connect()
        self.producer = self.client.create_producer(self.topic)


class PulsarSub(Pulsar):
    """Wrapper around pulsar.Consumer.

    Extends:
        Pulsar
    """

    def __init__(self, address: str, topic: str) -> None:
        super().__init__(address, topic)
        self.consumer = None  # type: pulsar.Consumer
        self.subscription_name = f'{self.topic}-subscription'  # single shared subscription
        self.prefetch = 1

    def connect(self) -> None:
        """Connect to subscriber."""
        super().connect()
        self.consumer = self.client.subscribe(self.topic,
                                              self.subscription_name,
                                              receiver_queue_size=self.prefetch,
                                              consumer_type=pulsar.ConsumerType.Shared,
                                              initial_position=pulsar.InitialPosition.Earliest)

    def close(self) -> None:
        """Close client and redeliver any unacknowledged messages."""
        if self.consumer:
            self.consumer.redeliver_unacknowledged_messages()
        super().close()


# Interface Methods


def create_pub_queue(address: str, name: str) -> PulsarPub:
    """Create a publishing queue."""
    q = PulsarPub(address, name)
    q.connect()
    return q


def create_sub_queue(address: str, name: str, prefetch: int = 1) -> PulsarSub:
    """Create a subscription queue."""
    q = PulsarSub(address, name)
    q.prefetch = prefetch
    q.connect()
    return q


def send_message(queue: PulsarPub, msg: bytes) -> None:
    """Send a message on a queue."""
    if not queue.producer:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.SENDING_MESSAGE)
    queue.producer.send(msg)
    logging.debug(log_msgs.SENT_MESSAGE)


def get_message(queue: PulsarSub, timeout_millis: int = 100) -> typing.Optional[Message]:
    """Get a single message from a queue.

    To endlessly block until a message is available, set
    `timeout_millis=None`.
    """
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
    for i in range(3):
        try:
            msg = queue.consumer.receive(timeout_millis=timeout_millis)
            if msg:
                message_id, data = msg.message_id(), msg.data()
                if message_id and data:
                    logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({message_id}).")
                    return Message(message_id, data)
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None

        except Exception as e:
            if str(e) == "Pulsar error: TimeOut":  # pulsar isn't a fan of derived Exceptions
                logging.debug(log_msgs.GETMSG_TIMEOUT_ERROR)
                return None
            if str(e) == "Pulsar error: AlreadyClosed":
                queue.close()
                time.sleep(1)
                queue.connect()
                logging.debug(f"{log_msgs.GETMSG_CONNECTION_ERROR_TRY_AGAIN} (try #{i+2})...")
                continue
            logging.debug(log_msgs.GETMSG_RAISE_OTHER_ERROR)
            raise

    logging.debug(log_msgs.GETMSG_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception('Pulsar connection error')


def ack_message(queue: PulsarSub, msg_id: MessageID) -> None:
    """Ack a message from the queue."""
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.ACKING_MESSAGE)
    queue.consumer.acknowledge(msg_id)
    logging.debug(log_msgs.ACKD_MESSAGE)


def reject_message(queue: PulsarSub, msg_id: MessageID) -> None:
    """Reject (nack) a message from the queue."""
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.NACKING_MESSAGE)
    queue.consumer.negative_acknowledge(msg_id)
    logging.debug(log_msgs.NACKD_MESSAGE)


def message_generator(queue: PulsarSub, timeout: int = 60, auto_ack: bool = True,
                      propagate_error: bool = True) -> typing.Generator[Message, None, None]:
    """Yield Messages.

    Arguments:
        queue {PulsarSub} -- queue object

    Keyword Arguments:
        timeout {int} -- timeout in seconds for inactivity (default: {60})
        auto_ack {bool} -- Ack each message after successful processing (default: {True})
        propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
    """
    if not queue.consumer:
        raise RuntimeError("queue is not connected")

    try:
        while True:
            logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
            msg = None
            try:
                msg = get_message(queue, timeout_millis=timeout * 1000)
                if msg is None:
                    logging.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                    break
                yield msg
            except Exception as e:  # pylint: disable=W0703
                if msg:
                    reject_message(queue, msg.msg_id)
                if propagate_error:
                    logging.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                    raise
                logging.warning(f"{log_msgs.MSGGEN_ERROR_DOWNSTREAM} {e}.", exc_info=True)
            else:
                if auto_ack:
                    ack_message(queue, msg.msg_id)
    finally:
        queue.close()
        logging.debug(log_msgs.MSGGEN_CLOSED_QUEUE)
