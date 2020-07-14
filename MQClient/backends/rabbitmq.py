"""Back-end using RabbitMQ."""

import logging
import time
from functools import partial
from typing import Any, Callable, Generator, Optional

import pika  # type: ignore

from .. import backend_interface
from ..backend_interface import Message, MessageID, Pub, RawQueue, Sub
from . import log_msgs


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


class RabbitMQPub(RabbitMQ, Pub):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        RabbitMQ
        Pub
    """

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        super(RabbitMQPub, self).connect()

        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.confirm_delivery()

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        if not self.channel:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.SENDING_MESSAGE)
        try_call(self, partial(self.channel.basic_publish, exchange='',
                               routing_key=self.queue, body=msg))
        logging.debug(log_msgs.SENT_MESSAGE)


class RabbitMQSub(RabbitMQ, Sub):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        RabbitMQ
        Sub
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

    def get_message(self) -> Optional[Message]:
        """Get a message from a queue."""
        if not self.channel:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        method_frame, _, body = try_call(self, partial(self.channel.basic_get, self.queue))

        if method_frame:
            msg = Message(method_frame.delivery_tag, body)
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({int(msg.msg_id)}).")
            return msg

        logging.debug(log_msgs.GETMSG_NO_MESSAGE)
        return None

    def ack_message(self, msg_id: MessageID) -> None:
        """Ack a message from the queue.

        Note that RabbitMQ acks messages in-order, so acking message
        3 of 3 in-progress messages will ack them all.

        Args:
            queue (RabbitMQSub): queue object
            msg_id (MessageID): message id
        """
        if not self.channel:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.ACKING_MESSAGE)
        try_call(self, partial(self.channel.basic_ack, msg_id))
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg_id!r}).")

    def reject_message(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue.

        Note that RabbitMQ acks messages in-order, so nacking message
        3 of 3 in-progress messages will nack them all.

        Args:
            queue (RabbitMQSub): queue object
            msg_id (MessageID): message id
        """
        if not self.channel:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.NACKING_MESSAGE)
        try_call(self, partial(self.channel.basic_nack, msg_id))
        logging.debug(f"{log_msgs.NACKED_MESSAGE} ({msg_id!r}).")

    def message_generator(self, timeout: int = 60, auto_ack: bool = True,
                          propagate_error: bool = True) -> Generator[Optional[Message], None, None]:
        """Yield Messages.

        Generate messages with variable timeout. Close instance on exit and error.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            auto_ack {bool} -- Ack each message after successful processing (default: {True})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        if not self.channel:
            raise RuntimeError("queue is not connected")

        msg = None
        acked = False
        try:
            gen = partial(self.channel.consume, self.queue, inactivity_timeout=timeout)

            for method_frame, _, body in try_yield(self, gen):
                # get message
                msg = None
                logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                if not method_frame:
                    logging.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                    break
                msg = Message(method_frame.delivery_tag, body)
                acked = False

                # yield message to consumer
                try:
                    logging.debug(f"{log_msgs.MSGGEN_YIELDING_MESSAGE} [{msg}]")
                    yield msg
                # consumer throws Exception...
                except Exception as e:  # pylint: disable=W0703
                    logging.debug(log_msgs.MSGGEN_DOWNSTREAM_ERROR)
                    if msg:
                        self.reject_message(msg.msg_id)
                    if propagate_error:
                        logging.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                        raise
                    logging.warning(f"{log_msgs.MSGGEN_EXCEPTED_DOWNSTREAM_ERROR} {e}.", exc_info=True)
                    yield None
                # consumer requests again, aka next()
                else:
                    if auto_ack:
                        self.ack_message(msg.msg_id)
                        acked = True

        # generator exit (explicit close(), or break in consumer's loop)
        except GeneratorExit:
            logging.debug(log_msgs.MSGGEN_GENERATOR_EXIT)
            if auto_ack and (not acked) and msg:
                self.ack_message(msg.msg_id)
                acked = True

        # generator is closed (also, garbage collected)
        finally:
            try_call(self, self.channel.cancel)
            logging.debug(log_msgs.MSGGEN_CLOSED_QUEUE)


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
        logging.debug(f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+2})...")

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
        logging.debug(f"{log_msgs.TRYYIELD_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+2})...")

    logging.debug(log_msgs.TRYYIELD_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception('RabbitMQ connection error')


class Backend(backend_interface.Backend):
    """RabbitMQ Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    @staticmethod
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

    @staticmethod
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
