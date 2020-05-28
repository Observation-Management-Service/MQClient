import logging
import time
import typing
from functools import partial

import pika  # type: ignore

from ..backend_interface import Message, MessageID, RawQueue

# Private Classes


class RabbitMQ(RawQueue):

    def __init__(self, address: str, queue: str) -> None:
        self.address = address
        if not self.address.startswith('ampq'):
            self.address = 'amqp://' + self.address
        self.queue = queue
        self.connection = None
        self.channel = None
        self.consumer_id = None
        self.prefetch = 1

    def connect(self):
        self.connection = pika.BlockingConnection(pika.connection.URLParameters(self.address))
        self.channel = self.connection.channel()

    def close(self):
        if (self.connection) and (not self.connection.is_closed):
            self.connection.close()


class RabbitMQPub(RabbitMQ):

    def connect(self):
        super(RabbitMQPub, self).connect()
        # Turn on delivery confirmations
        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.confirm_delivery()


class RabbitMQSub(RabbitMQ):

    def __init__(self, *args, **kwargs) -> None:
        super(RabbitMQSub, self).__init__(*args, **kwargs)
        self.consumer_id = None
        self.prefetch = 1

    def connect(self):
        super(RabbitMQSub, self).connect()
        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.basic_qos(prefetch_count=self.prefetch, global_qos=True)


def try_call(queue: RabbitMQ, func: typing.Callable) -> typing.Any:
    for _ in range(3):
        try:
            return func()
        except pika.exceptions.ConnectionClosedByBroker:
            pass
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error("Caught a channel error: {}, stopping...".format(err))
            raise
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            pass
        queue.close()
        time.sleep(1)
        queue.connect()
    raise Exception('RabbitMQ connection error')


def try_yield(queue: RabbitMQ, func: typing.Callable) -> typing.Generator[typing.Any, None, None]:
    for _ in range(3):
        try:
            for x in func():
                yield x
        except pika.exceptions.ConnectionClosedByBroker:
            pass
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error("Caught a channel error: {}, stopping...".format(err))
            raise
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            pass
        queue.close()
        time.sleep(1)
        queue.connect()
    raise Exception('RabbitMQ connection error')

# Interface Methods


def create_pub_queue(address: str, name: str) -> RabbitMQPub:
    """
    Create a publishing queue

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
    """Create a subscription queue

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
    """
    Send a message on a queue

    Args:
        address (str): address of queue
        name (str): name of queue on address

    Returns:
        RawQueue: queue
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")

    try_call(queue, partial(queue.channel.basic_publish, exchange='',
                            routing_key=queue.queue, body=msg))


def get_message(queue: RabbitMQSub) -> typing.Optional[Message]:
    """Get a message from a queue"""
    if not queue.channel:
        raise RuntimeError("queue is not connected")
    method_frame, header_frame, body = try_call(
        queue, partial(queue.channel.basic_get, queue.queue))
    if method_frame:
        return Message(method_frame.delivery_tag, body)


def ack_message(queue: RabbitMQSub, msg_id: MessageID) -> None:
    """
    Ack a message from the queue.

    Note that RabbitMQ acks messages in-order, so acking message
    3 of 3 in-progress messages will ack them all.

    Args:
        queue (RabbitMQSub): queue object
        msg_id (MessageID): message id
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")
    try_call(queue, partial(queue.channel.basic_ack, msg_id))


def reject_message(queue: RabbitMQSub, msg_id: MessageID) -> None:
    """
    Reject (nack) a message from the queue.

    Note that RabbitMQ acks messages in-order, so nacking message
    3 of 3 in-progress messages will nack them all.

    Args:
        queue (RabbitMQSub): queue object
        msg_id (MessageID): message id
    """
    if not queue.channel:
        raise RuntimeError("queue is not connected")
    try_call(queue, partial(queue.channel.basic_nack, msg_id))


def message_generator(queue: RabbitMQSub, timeout: int = 60, auto_ack: bool = True,
                      propagate_error: bool = True) -> typing.Generator[Message, None, None]:
    """
    A generator yielding a Message.

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
        for method_frame, header_frame, body in try_yield(queue, gen):
            if not method_frame:
                logging.info("no messages in idle timeout window")
                break
            try:
                yield Message(method_frame.delivery_tag, body)
            except Exception as e:
                try_call(queue, partial(queue.channel.basic_nack, method_frame.delivery_tag))
                if propagate_error:
                    raise
                else:
                    logging.warn('error downstream: %r', e, exc_info=True)
            else:
                if auto_ack:
                    try_call(queue, partial(queue.channel.basic_ack, method_frame.delivery_tag))
    finally:
        try_call(queue, queue.channel.cancel)
