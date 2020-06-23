"""Back-end using RabbitMQ."""

import logging
import time
from functools import partial
from typing import Any, Callable, Generator, Optional

import pika  # type: ignore

from ..backend_interface import Message, MessageID, RawQueue
from . import log_msgs

# Private Classes


class RabbitMQ(RawQueue):
    """Base RabbitMQ wrapper.

    Extends:
        RawQueue
    """

    def __init__(self, address: str, queue: str) -> None:
        self.address = address
        if not self.address.startswith('ampq'):
            self.address = 'amqp://' + self.address
        self.queue = queue
        self.connection = None  # type: pika.BlockingConnection
        self.channel = None  # type: pika.adapters.blocking_connection.BlockingChannel
        self.consumer_id = None
        self.prefetch = 1

    def connect(self) -> None:
        """Set up connection and channel."""
        self.connection = pika.BlockingConnection(pika.connection.URLParameters(self.address))
        self.channel = self.connection.channel()

    def close(self) -> None:
        """Close connection."""
        if (self.connection) and (not self.connection.is_closed):
            self.connection.close()


class RabbitMQPub(RabbitMQ):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        RabbitMQ
    """

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        super(RabbitMQPub, self).connect()

        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.confirm_delivery()


class RabbitMQSub(RabbitMQ):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        RabbitMQ
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(RabbitMQSub, self).__init__(*args, **kwargs)

        self.consumer_id = None
        self.prefetch = 1

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on prefetching.
        """
        super(RabbitMQSub, self).connect()

        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.basic_qos(prefetch_count=self.prefetch, global_qos=True)


def try_call(queue: RabbitMQ, func: Callable[..., Any]) -> Any:
    """Try to call `func` and return value.

    Try up to 3 times, for connection-related errors.
    """
    for i in range(3):
        try:
            return func()
        except pika.exceptions.ConnectionClosedByBroker:
            logging.debug(log_msgs.TRYCALL_CONNECTION_CLOSED_BY_BROKER)
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error(f"{log_msgs.TRYCALL_RAISE_AMQP_CHANNEL_ERROR} {err}.")
            raise
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            logging.debug(log_msgs.TRYCALL_AMQP_CONNECTION_ERROR)

        queue.close()
        time.sleep(1)
        queue.connect()
        logging.debug(f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (try #{i+2})...")

    logging.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception('RabbitMQ connection error')


def try_yield(queue: RabbitMQ, func: Callable[..., Any]) -> Generator[Any, None, None]:
    """Try to call `func` and yield value(s).

    Try up to 3 times, for connection-related errors.
    """
    for i in range(3):
        try:
            for x in func():
                yield x
        except pika.exceptions.ConnectionClosedByBroker:
            logging.debug(log_msgs.TRYYIELD_CONNECTION_CLOSED_BY_BROKER)
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error(f"{log_msgs.TRYYIELD_RAISE_AMQP_CHANNEL_ERROR} {err}.")
            raise
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            logging.debug(log_msgs.TRYYIELD_AMQP_CONNECTION_ERROR)

        queue.close()
        time.sleep(1)
        queue.connect()
        logging.debug(f"{log_msgs.TRYYIELD_CONNECTION_ERROR_TRY_AGAIN} (try #{i+2})...")

    logging.debug(log_msgs.TRYYIELD_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception('RabbitMQ connection error')


# Interface Methods


def create_pub_queue(address: str, name: str) -> RabbitMQPub:
    """Create a publishing queue.

    Args:
        address (str): address of queue
        name (str): name of queue on address

    Returns:
        RawQueue: queue
    """
    q = RabbitMQPub(address, name)
    q.connect()
    return q


def create_sub_queue(address: str, name: str, prefetch: int = 1) -> RabbitMQSub:
    """Create a subscription queue.

    Args:
        address (str): address of queue
        name (str): name of queue on address

    Returns:
        RawQueue: queue
    """
    q = RabbitMQSub(address, name)
    q.prefetch = prefetch
    q.connect()
    return q


def send_message(queue: RabbitMQPub, msg: bytes) -> None:
    """Send a message on a queue.

    Args:
        address (str): address of queue
        name (str): name of queue on address

    Returns:
        RawQueue: queue
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.SENDING_MESSAGE)
    try_call(queue, partial(queue.channel.basic_publish, exchange='',
                            routing_key=queue.queue, body=msg))
    logging.debug(log_msgs.SENT_MESSAGE)


def get_message(queue: RabbitMQSub) -> Optional[Message]:
    """Get a message from a queue."""
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
    method_frame, _, body = try_call(queue, partial(queue.channel.basic_get, queue.queue))

    if method_frame:
        msg = Message(method_frame.delivery_tag, body)
        logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({int(msg.msg_id)}).")
        return msg

    logging.debug(log_msgs.GETMSG_NO_MESSAGE)
    return None


def ack_message(queue: RabbitMQSub, msg_id: MessageID) -> None:
    """Ack a message from the queue.

    Note that RabbitMQ acks messages in-order, so acking message
    3 of 3 in-progress messages will ack them all.

    Args:
        queue (RabbitMQSub): queue object
        msg_id (MessageID): message id
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.ACKING_MESSAGE)
    try_call(queue, partial(queue.channel.basic_ack, msg_id))
    logging.debug(log_msgs.ACKD_MESSAGE)


def reject_message(queue: RabbitMQSub, msg_id: MessageID) -> None:
    """Reject (nack) a message from the queue.

    Note that RabbitMQ acks messages in-order, so nacking message
    3 of 3 in-progress messages will nack them all.

    Args:
        queue (RabbitMQSub): queue object
        msg_id (MessageID): message id
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    logging.debug(log_msgs.NACKING_MESSAGE)
    try_call(queue, partial(queue.channel.basic_nack, msg_id))
    logging.debug(log_msgs.NACKD_MESSAGE)


def message_generator(queue: RabbitMQSub, timeout: int = 60, auto_ack: bool = True,
                      propagate_error: bool = True) -> Generator[Message, None, None]:
    """Yield a Message.

    Args:
        queue (RabbitMQSub): queue object
        timeout (int): timeout in seconds for inactivity
        auto_ack (bool): Ack each message after successful processing
        propagate_error (bool): should errors from downstream code kill the generator?
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    try:
        gen = partial(queue.channel.consume, queue.queue, inactivity_timeout=timeout)

        for method_frame, _, body in try_yield(queue, gen):
            logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
            if not method_frame:
                logging.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                break

            try:
                yield Message(method_frame.delivery_tag, body)
            except Exception as e:  # pylint: disable=W0703
                try_call(queue, partial(queue.channel.basic_nack, method_frame.delivery_tag))
                if propagate_error:
                    logging.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                    raise
                logging.warning(f"{log_msgs.MSGGEN_ERROR_DOWNSTREAM} {e}.", exc_info=True)
            else:
                if auto_ack:
                    try_call(queue, partial(queue.channel.basic_ack, method_frame.delivery_tag))
    finally:
        try_call(queue, queue.channel.cancel)
        logging.debug(log_msgs.MSGGEN_CLOSED_QUEUE)
