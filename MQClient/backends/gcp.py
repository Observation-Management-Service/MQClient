"""Back-end using GCP."""

import logging
import os

# from functools import partial
from typing import Generator, Optional

from google.api_core import exceptions  # type: ignore[import]
from google.cloud import pubsub_v1 as api  # type: ignore[import]

from .. import backend_interface
from ..backend_interface import GET_MSG_TIMEOUT, Message, MessageID, Pub, RawQueue, Sub
from . import log_msgs


class GCP(RawQueue):
    """Base GCP wrapper.

    Extends:
        RawQueue
    """

    def __init__(self, endpoint: str, project_id: str, topic_id: str) -> None:
        super().__init__()
        self.endpoint = endpoint
        self._project_id = project_id

        # create a temporary PublisherClient just to get `topic_path`
        self._topic_path = api.PublisherClient().topic_path(  # pylint: disable=no-member
            self._project_id, topic_id
        )
        logging.debug(f"Topic Path: {self._topic_path}")

    def connect(self) -> None:
        """Set up connection and channel."""
        super().connect()

    def close(self) -> None:
        """Close connection."""
        super().close()


class GCPPub(GCP, Pub):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        GCP
        Pub
    """

    def __init__(self, endpoint: str, project_id: str, topic_id: str):
        logging.debug(f"{log_msgs.INIT_PUB} ({endpoint}; {project_id}; {topic_id})")
        super().__init__(endpoint, project_id, topic_id)
        self.pub: Optional[api.PublisherClient] = None

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        logging.debug(log_msgs.CONNECTING_PUB)
        super().connect()

        self.pub = api.PublisherClient(client_options={"api_endpoint": self.endpoint})
        # publisher_options=api.types.PublisherOptions(enable_message_ordering=True),

        try:
            self.pub.create_topic(self._topic_path)  # pylint: disable=no-member
        except exceptions.AlreadyExists:
            logging.debug(f"{log_msgs.TOPIC_ALREADY_EXISTS} ({self._topic_path})")
        finally:
            logging.debug(log_msgs.CONNECTED_PUB)

    def close(self) -> None:
        """Close connection."""
        logging.debug(log_msgs.CLOSING_PUB)
        super().close()
        logging.debug(log_msgs.CLOSED_PUB)

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue."""
        logging.debug(log_msgs.SENDING_MESSAGE)
        if not self.pub:
            raise RuntimeError("publisher is not connected")

        # try_call(self, partial(self.publisher.publish, self.topic_path, msg)) # TODO
        self.pub.publish(self._topic_path, msg)
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
        logging.debug(
            f"{log_msgs.INIT_SUB} "
            f"({endpoint}; {project_id}; {topic_id}; {subscription_id})"
        )
        super().__init__(endpoint, project_id, topic_id)
        self.sub: Optional[api.SubscriberClient] = None
        self.prefetch = 1

        self._subscription_path: Optional[str] = None
        self._subscription_id = subscription_id

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on prefetching.

        NOTE: Based on `examples/gcp/subscriber.create_subscription()`
        """
        logging.debug(log_msgs.CONNECTING_SUB)
        super().connect()

        self.sub = api.SubscriberClient(client_options={"api_endpoint": self.endpoint})
        self._subscription_path = self.sub.subscription_path(  # pylint: disable=no-member
            self._project_id, self._subscription_id
        )

        try:
            self.sub.create_subscription(  # pylint: disable=no-member
                self._subscription_path, self._topic_path
            )
        except exceptions.AlreadyExists:
            logging.debug(
                f"{log_msgs.SUBSCRIPTION_ALREADY_EXISTS} ({self._subscription_path})"
            )
        finally:
            logging.debug(log_msgs.CONNECTED_SUB)

    def close(self) -> None:
        """Close connection."""
        logging.debug(log_msgs.CLOSING_SUB)
        super().close()
        if self.sub:
            self.sub.close()
        logging.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: api.types.ReceivedMessage  # pylint: disable=no-member
    ) -> Optional[Message]:
        """Transform GCP-Message to Message type."""
        return Message(msg.ack_id, msg.message.data)

    def _get_messages(
        self, timeout_millis: Optional[int], num_messages: int
    ) -> Generator[Message, None, None]:
        """Get n messages.

        The subscriber pulls a specific number of messages. The actual
        number of messages pulled may be smaller than max_messages.
        """
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        response = self.sub.pull(  # pylint: disable=no-member
            subscription=self._subscription_path,
            max_messages=num_messages,
            # retry=retry.Retry(deadline=300),  # TODO
            # return_immediately=True, # NOTE - use is discourage for performance reasons
            timeout=timeout_millis / 1000 if timeout_millis else 0,
            # NOTE - if `retry` is specified, the timeout applies to each individual attempt
        )

        # Yield Each Message
        for recvd in response.received_messages:
            msg = GCPSub._to_message(recvd)
            if msg:
                yield msg

    def get_message(
        self, timeout_millis: Optional[int] = GET_MSG_TIMEOUT
    ) -> Optional[Message]:
        """Get a message.

        NOTE: Based on `examples/gcp/subscriber.synchronous_pull()`
        """
        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        msg = next(self._get_messages(timeout_millis, 1), None)

        # Process & Return
        if not msg:  # NOTE - on timeout -> this will be len=0
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None  # kind of redundant
        else:  # got 1 message
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg.msg_id!r}).")
            return msg

    def ack_message(self, msg_id: MessageID) -> None:
        """Ack a message from the queue."""
        logging.debug(log_msgs.ACKING_MESSAGE)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        # Acknowledges the received messages so they will not be sent again.
        self.sub.acknowledge(  # pylint: disable=no-member
            subscription=self._subscription_path, ack_ids=[msg_id]
        )
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg_id!r}).")

    def reject_message(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue."""
        logging.debug(log_msgs.NACKING_MESSAGE)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        # TODO - messages are auto-nacked(?)
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
        # TODO/FIXME
        logging.debug(log_msgs.MSGGEN_ENTERED)
        if not self.sub:
            raise RuntimeError("subscriber is not connected")

        msg = None
        acked = False
        try:
            gen = self._get_messages(timeout * 1000, self.prefetch)
            while True:
                # get message
                logging.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = next(gen, None)
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
                    logging.warning(
                        f"{log_msgs.MSGGEN_EXCEPTED_DOWNSTREAM_ERROR} {e}.",
                        exc_info=True,
                    )
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
    """GCP Pub-Sub Backend Factory.

    Extends:
        Backend
    """

    # NOTE - this could be an enviro var, but it is always constant across all members
    PROJECT_ID = "i3-gcp-proj"

    # NOTE - use single shared subscription
    # (making multiple unique subscription ids would create independent subscriptions)
    # See https://thecloudgirl.dev/images/pubsub.jpg
    SUBSCRIPTION_ID = "i3-gcp-sub"

    # NOTE - this is an environment variable, which should override the host address
    PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST"

    @staticmethod
    def _figure_host_address(address: str) -> str:
        """If the pub-sub emulator enviro var is set, use that address."""
        try:
            address = os.environ[Backend.PUBSUB_EMULATOR_HOST]
            logging.debug(f"GCP-Backend: Using Pub-Sub Emulator at {address}.")
        except KeyError:
            pass

        return address

    @staticmethod
    def create_pub_queue(address: str, name: str) -> GCPPub:
        """Create a publishing queue."""
        # pylint: disable=invalid-name
        q = GCPPub(Backend._figure_host_address(address), Backend.PROJECT_ID, name)
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(address: str, name: str, prefetch: int = 1) -> GCPSub:
        """Create a subscription queue."""
        q = GCPSub(  # pylint: disable=invalid-name
            Backend._figure_host_address(address),
            Backend.PROJECT_ID,
            name,
            Backend.SUBSCRIPTION_ID,
        )
        q.prefetch = prefetch
        q.connect()
        return q
