"""Back-end using GCP."""

import logging
import time
from functools import partial
from typing import Any, Callable, Final, Generator, Optional

from google.api_core import retry  # type: ignore[import]
from google.cloud import pubsub_v1 as gcp_v1  # type: ignore[import]

from .. import backend_interface
from ..backend_interface import Message, MessageID, Pub, RawQueue, Sub
from . import log_msgs


class GCP(RawQueue):
    """Base GCP wrapper.

    Extends:
        RawQueue
    """

    def __init__(
        self, endpoint: str, project_id: str, topic_id: str, subscription_id: str
    ) -> None:
        super().__init__()

        # self.address = address
        # if not self.address.startswith("ampq"):
        #     self.address = "amqp://" + self.address
        # self.queue = queue

        self._proj_id = project_id
        self._topic_id = topic_id
        self._sub_id = subscription_id

        self._push_config = gcp_v1.types.PushConfig(push_endpoint=endpoint)

        # self.connection = None  # type: pika.BlockingConnection
        # self.channel = None  # type: pika.adapters.blocking_connection.BlockingChannel
        # self.consumer_id = None
        # self.prefetch = 1

    def connect(self) -> None:
        """Set up connection and channel."""
        super().connect()
        # self.connection = pika.BlockingConnection(
        #     pika.connection.URLParameters(self.address)
        # )
        # self.channel = self.connection.channel()

    def close(self) -> None:
        """Close connection."""
        super().close()
        # if (self.connection) and (not self.connection.is_closed):
        #     self.connection.close()


class GCPPub(GCP, Pub):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        GCP
        Pub
    """

    def __init__(
        self, endpoint: str, project_id: str, topic_id: str, subscription_id: str
    ):
        super().__init__(endpoint, project_id, topic_id, subscription_id)
        self.publisher: Optional[gcp_v1.PublisherClient] = None

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        super().connect()
        self.publisher = gcp_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self._proj_id, self._topic_id)
        print(f"{topic_path=}")
        topic = self.publisher.create_topic(topic_path)
        print(f"Created topic: {topic.name} -- {topic}")

        # self.channel.queue_declare(queue=self.queue, durable=False)
        # self.channel.confirm_delivery()

    def send_msg(self, msg: bytes) -> None:
        """Send a message on a queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        if not self.publisher:
            raise RuntimeError("publisher is not connected")

        logging.debug(log_msgs.SENDING_MESSAGE)
        try_call(self, partial(self.publisher.publish, self.topic_path, msg))
        # TODO - call-back? this return a Future:
        # publish_future.add_done_callback(get_callback(publish_future, data))
        # futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        logging.debug(log_msgs.SENT_MESSAGE)


class GCPSub(GCP, Sub):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        GCP
        Sub
    """

    def __init__(
        self, endpoint: str, project_id: str, topic_id: str, subscription_id: str
    ):
        super().__init__(endpoint, project_id, topic_id, subscription_id)
        self._sub_path: Optional[str] = None
        # self.subscriber: Optional[gcp_v1.SubscriberClient] = None
        print(f"{project_id=} {topic_id=} {subscription_id=}")
        # self.consumer_id = None
        # self.prefetch = 1

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on prefetching.
        """
        super().connect()

        # NOTE: From create_subscription()

        subscriber = gcp_v1.SubscriberClient()
        topic_path = gcp_v1.PublisherClient().topic_path(self._proj_id, self._topic_id)
        print(f"{topic_path=}")
        subscription_path = subscriber.subscription_path(self._proj_id, self._sub_id)

        # Wrap the subscriber in a 'with' block to automatically call close() to
        # close the underlying gRPC channel when done.
        with subscriber:
            # subscription = subscriber.create_subscription(
            #     request={"name": subscription_path, "topic": topic_path}
            # )
            # TODO - https://github.com/googleapis/python-pubsub/issues/182#issuecomment-690951537
            subscription = subscriber.create_subscription(subscription_path, topic_path)

        print(f"Subscription created: {subscription}")
        # [END pubsub_create_pull_subscription]

    def close(self) -> None:
        """Close connection."""
        super().close()
        # if self.subscriber:
        # self.subscriber.close()

    def get_msg(self) -> Optional[Message]:
        """Get a message from a queue."""
        # if not self.subscriber:
        # raise RuntimeError("subscriber is not connected")
        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)

        # NOTE: From synchronous_pull()

        subscriber = gcp_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(self._proj_id, self._sub_id)

        # Wrap the subscriber in a 'with' block to automatically call close() to

        num_messages: Final[int] = 1

        # close the underlying gRPC channel when done.
        with subscriber:
            # The subscriber pulls a specific number of messages. The actual
            # number of messages pulled may be smaller than max_messages.
            response = subscriber.pull(
                # request={
                #     "subscription": subscription_path,
                #     "max_messages": num_messages,
                # }, # TODO
                subscription=subscription_path,
                max_messages=num_messages,
                retry=retry.Retry(deadline=300),
            )

            ack_ids = []
            msgs = []
            for received_message in response.received_messages:
                print(f"Received: {received_message.message.data}.")
                logging.debug(
                    f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({received_message.message.data})."
                )  # TODO
                ack_ids.append(received_message.ack_id)
                msgs.append(received_message)  # TODO

            # NOTE - on timeout -> this will be len=0
            assert len(ack_ids) == 1  # TODO
            # logging.debug(log_msgs.GETMSG_NO_MESSAGE) # TODO

            # Acknowledges the received messages so they will not be sent again.
            subscriber.acknowledge(
                # request={"subscription": subscription_path, "ack_ids": ack_ids}
                subscription=subscription_path,
                ack_ids=ack_ids,
            )

            print(
                f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
            )

        return msgs[0]
        # [END pubsub_subscriber_sync_pull]

    def ack_msg(self, message: Any) -> None:  # TODO - figure type of `message`
        """Ack a message from the queue.

        Note that GCP acks messages in-order, so acking message
        3 of 3 in-progress messages will ack them all.

        Args:
            queue (GCPSub): queue object
            msg_id (MessageID): message id
        """
        # FIXME / TODO
        if not self.subscriber:
            raise RuntimeError("subscriber is not connected")
        if not message:
            raise RuntimeError("there was no message to acknowledge")

        logging.debug(log_msgs.ACKING_MESSAGE)
        try_call(self, partial(message.ack))
        # NOTE: if this doesn't work, try:
        # subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({message!r}).")

    def reject_msg(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue.

        Note that GCP acks messages in-order, so nacking message
        3 of 3 in-progress messages will nack them all.

        Args:
            queue (GCPSub): queue object
            msg_id (MessageID): message id
        """
        # FIXME / TODO
        if not self.channel:
            raise RuntimeError("queue is not connected")

        logging.debug(log_msgs.NACKING_MESSAGE)
        # try_call(self, partial(self.channel.basic_nack, msg_id))
        logging.debug(f"{log_msgs.NACKED_MESSAGE} ({msg_id!r}).")

    def message_generator(
        self, timeout: int = 60, auto_ack: bool = True, propagate_error: bool = True
    ) -> Generator[Optional[Message], None, None]:
        """Yield Messages.

        Generate messages with variable timeout. Close instance on exit and error.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            auto_ack {bool} -- Ack each message after successful processing (default: {True})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        # TODO - use: streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        # FIXME / TODO
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
                        self.reject_msg(msg.msg_id)
                    if propagate_error:
                        logging.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                        raise
                    logging.warning(
                        f"{log_msgs.MSGGEN_EXCEPTED_DOWNSTREAM_ERROR} {e}.",
                        exc_info=True,
                    )
                    yield None
                # consumer requests again, aka next()
                else:
                    if auto_ack:
                        self.ack_msg(msg.msg_id)
                        acked = True

        # generator exit (explicit close(), or break in consumer's loop)
        except GeneratorExit:
            logging.debug(log_msgs.MSGGEN_GENERATOR_EXIT)
            if auto_ack and (not acked) and msg:
                self.ack_msg(msg.msg_id)
                acked = True

        # generator is closed (also, garbage collected)
        finally:
            try_call(self, self.channel.cancel)
            self.was_closed = True
            logging.debug(log_msgs.MSGGEN_CLOSED_QUEUE)


def try_call(queue: GCP, func: Callable[..., Any]) -> Any:
    """Try to call `func` and return value.

    Try up to 3 times, for connection-related errors.
    """
    for i in range(3):
        if i > 0:
            logging.debug(
                f"{log_msgs.TRYCALL_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+1})..."
            )

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

    logging.debug(log_msgs.TRYCALL_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception("GCP connection error")


def try_yield(queue: GCP, func: Callable[..., Any]) -> Generator[Any, None, None]:
    """Try to call `func` and yield value(s).

    Try up to 3 times, for connection-related errors.
    """
    for i in range(3):
        if i > 0:
            logging.debug(
                f"{log_msgs.TRYYIELD_CONNECTION_ERROR_TRY_AGAIN} (attempt #{i+1})..."
            )

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

    logging.debug(log_msgs.TRYYIELD_CONNECTION_ERROR_MAX_RETRIES)
    raise Exception("GCP connection error")


class Backend(backend_interface.Backend):
    """GCP Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    @staticmethod
    def create_pub_queue(address: str, name: str) -> GCPPub:
        """Create a publishing queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        q = GCPPub(address, name)
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(address: str, name: str, prefetch: int = 1) -> GCPSub:
        """Create a subscription queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        q = GCPSub(address, name)
        q.prefetch = prefetch
        q.connect()
        return q
