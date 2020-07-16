"""Back-end using Apache Pulsar."""

import logging
import time
from typing import Generator, Optional

import pulsar  # type: ignore

from .. import backend_interface
from ..backend_interface import Message, MessageID, Pub, RawQueue, Sub
from . import log_msgs


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


class PulsarPub(Pulsar, Pub):
    """Wrapper around pulsar.Producer.

    Extends:
        Pulsar
        Pub
    """

    def __init__(self, address: str, topic: str) -> None:
        super().__init__(address, topic)
        self.producer = None  # type: pulsar.Producer

    def connect(self) -> None:
        """Connect to producer."""
        super().connect()
        self.producer = self.client.create_producer(self.topic)

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue."""
        if not self.producer:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.SENDING_MESSAGE)
        self.producer.send(msg)
        logging.debug(log_msgs.SENT_MESSAGE)


class PulsarSub(Pulsar, Sub):
    """Wrapper around pulsar.Consumer.

    Extends:
        Pulsar
        Sub
    """

    def __init__(self, address: str, topic: str, max_retries: int) -> None:
        super().__init__(address, topic)
        self.consumer = None  # type: pulsar.Consumer
        self.subscription_name = f'{self.topic}-subscription'  # single shared subscription
        self.prefetch = 1
        self.max_retries = max_retries

    def connect(self) -> None:
        """Connect to subscriber."""
        super().connect()
        self.consumer = self.client.subscribe(self.topic,
                                              self.subscription_name,
                                              receiver_queue_size=self.prefetch,
                                              consumer_type=pulsar.ConsumerType.Shared,
                                              initial_position=pulsar.InitialPosition.Earliest,
                                              negative_ack_redelivery_delay_ms=0)

    def close(self) -> None:
        """Close client and redeliver any unacknowledged messages."""
        if self.consumer:
            self.consumer.redeliver_unacknowledged_messages()
        super().close()

    def get_message(self, timeout_millis: int = 100) -> Optional[Message]:
        """Get a single message from a queue.

        Retry up to `self.max_retries` times, for connection-related
        errors. To endlessly block until a message is available, set
        `timeout_millis=None`.
        """
        if not self.consumer:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        for i in range(self.max_retries + 1):
            try:
                msg = self.consumer.receive(timeout_millis=timeout_millis)
                if msg:
                    message_id, data = msg.message_id(), msg.data()
                    if (message_id is not None) and (data is not None):  # message_id may be 0; data may be b''
                        if isinstance(message_id, pulsar._pulsar.MessageId):  # pylint: disable=I1101,W0212
                            _id = message_id.serialize()  # message_id.serialize() -> bytes
                            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({_id!r}).")
                            return Message(_id, data)
                        logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({message_id}).")
                        return Message(message_id, data)
                logging.debug(log_msgs.GETMSG_NO_MESSAGE)
                return None

            except Exception as e:
                if str(e) == "Pulsar error: TimeOut":  # pulsar isn't a fan of derived Exceptions
                    logging.debug(log_msgs.GETMSG_TIMEOUT_ERROR)
                    return None
                if str(e) == "Pulsar error: AlreadyClosed":
                    self.close()
                    time.sleep(1)
                    self.connect()
                    logging.debug(f"{log_msgs.GETMSG_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+2})...")
                    continue
                logging.debug(f"{log_msgs.GETMSG_RAISE_OTHER_ERROR} ({e.__class__.__name__}).")
                raise

        logging.debug(log_msgs.GETMSG_CONNECTION_ERROR_MAX_RETRIES)
        raise Exception('Pulsar connection error')

    def ack_message(self, msg_id: MessageID) -> None:
        """Ack a message from the queue."""
        if not self.consumer:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.ACKING_MESSAGE)
        if isinstance(msg_id, bytes):
            self.consumer.acknowledge(pulsar.MessageId.deserialize(msg_id))
        else:
            self.consumer.acknowledge(msg_id)
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg_id!r}).")

    def reject_message(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue."""
        if not self.consumer:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.NACKING_MESSAGE)
        if isinstance(msg_id, bytes):
            self.consumer.negative_acknowledge(pulsar.MessageId.deserialize(msg_id))
        else:
            self.consumer.negative_acknowledge(msg_id)
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
        if not self.consumer:
            raise RuntimeError("queue is not connected")

        msg = None
        acked = False
        try:
            while True:
                # get message
                logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = self.get_message(timeout_millis=timeout * 1000)
                acked = False
                if msg is None:
                    logging.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                    break

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
            self.close()
            logging.debug(log_msgs.MSGGEN_CLOSED_QUEUE)


class Backend(backend_interface.Backend):
    """Pulsar Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    @staticmethod
    def create_pub_queue(address: str, name: str) -> PulsarPub:
        """Create a publishing queue."""
        q = PulsarPub(address, name)
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(address: str, name: str, prefetch: int = 1) -> PulsarSub:
        """Create a subscription queue."""
        q = PulsarSub(address, name, max_retries=Backend.max_retries)
        q.prefetch = prefetch
        q.connect()
        return q
