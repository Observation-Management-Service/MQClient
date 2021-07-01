"""Back-end using GCP."""

import logging

# from functools import partial
from typing import Final, Generator, List, Optional

from google.api_core import exceptions  # type: ignore[import]
from google.cloud import pubsub_v1 as gcp_v1  # type: ignore[import]

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

        self._proj_id = project_id

        self._push_config = gcp_v1.types.PushConfig(push_endpoint=endpoint)
        # self.prefetch = 1

        # create a temporary PublisherClient just to get `topic_path`
        self._topic_path = gcp_v1.PublisherClient().topic_path(self._proj_id, topic_id)
        print(f"{self._topic_path=}")

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
        super().__init__(endpoint, project_id, topic_id)
        self.publisher: Optional[gcp_v1.PublisherClient] = None

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        super().connect()
        self.publisher = gcp_v1.PublisherClient()
        try:
            topic = self.publisher.create_topic(self._topic_path)
            print(f"Created topic: {topic.name} -- {topic}")
        except exceptions.AlreadyExists:
            print(f"Topic already exists: {self._topic_path}")

    def send_message(self, msg: bytes) -> None:
        """Send a message on a queue."""
        if not self.publisher:
            raise RuntimeError("publisher is not connected")

        logging.debug(log_msgs.SENDING_MESSAGE)
        # try_call(self, partial(self.publisher.publish, self.topic_path, msg)) # TODO
        future = self.publisher.publish(self._topic_path, msg)
        print(f"{future.result()=}")
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
        super().__init__(endpoint, project_id, topic_id)

        self.subscriber: Optional[gcp_v1.SubscriberClient] = None

        self._sub_path: Optional[str] = None
        self._sub_id = subscription_id

        print(f"{project_id=} {topic_id=} {subscription_id=}")

        # self.consumer_id = None
        # self.prefetch = 1

    def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on prefetching.
        """
        super().connect()

        # NOTE: From create_subscription()

        self.subscriber = gcp_v1.SubscriberClient()
        self._sub_path = self.subscriber.subscription_path(self._proj_id, self._sub_id)

        # Wrap the subscriber in a 'with' block to automatically call close() to
        # close the underlying gRPC channel when done.
        # with subscriber:
        # subscription = subscriber.create_subscription(
        #     request={"name": subscription_path, "topic": topic_path}
        # )
        # TODO - https://github.com/googleapis/python-pubsub/issues/182#issuecomment-690951537
        # subscription = subscriber.create_subscription(sub_path, self._topic_path)
        # NOTE - not auto-closing `subscriber`
        try:
            subscription = self.subscriber.create_subscription(
                self._sub_path, self._topic_path
            )
            print(f"Subscription created: {subscription}")
        except exceptions.AlreadyExists:
            print(f"Subscription already exists: {self._sub_path}")
        # [END pubsub_create_pull_subscription]

    def close(self) -> None:
        """Close connection."""
        super().close()
        if self.subscriber:
            self.subscriber.close()

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: gcp_v1.types.ReceivedMessage
    ) -> Optional[Message]:
        """Transform GCP-Message to Message type."""
        return Message(msg.ack_id, msg.message.data)

    def get_message(
        self, timeout_millis: Optional[int] = GET_MSG_TIMEOUT
    ) -> Optional[Message]:
        """Get a message.

        NOTE: Based on `examples/gcp/subscriber.synchronous_pull()`
        """
        if not self.subscriber:
            raise RuntimeError("subscriber is not connected")

        logging.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)

        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        num_messages: Final[int] = 1

        response = self.subscriber.pull(
            subscription=self._sub_path,
            max_messages=num_messages,  # TODO - is this prefetch? (see above)
            # retry=retry.Retry(deadline=300),
            # return_immediately=True, # NOTE - use is discourage for performance reasons
            timeout=timeout_millis * 1000,
            # NOTE - if `retry` is specified, the timeout applies to each individual attempt
        )

        # Get Message(s)
        msgs: List[Message] = []
        for recvd in response.received_messages:
            msgs.append(GCPSub._to_message(recvd))

        # Process & Return
        if not msgs:  # NOTE - on timeout -> this will be len=0
            logging.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None
        elif len(msgs) > 1:
            raise RuntimeError("Received too many messages.")
        else:  # got 1 message
            logging.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msgs[0].data!r}).")
            return msgs[0]
        # [END pubsub_subscriber_sync_pull]

    def ack_message(self, msg_id: MessageID) -> None:
        """Ack a message from the queue."""
        if not self.subscriber:
            raise RuntimeError("subscriber is not connected")

        logging.debug(log_msgs.ACKING_MESSAGE)
        # Acknowledges the received messages so they will not be sent again.
        self.subscriber.acknowledge(subscription=self._sub_path, ack_ids=[msg_id])
        logging.debug(f"{log_msgs.ACKED_MESSAGE} ({msg_id!r}).")

    def reject_message(self, msg_id: MessageID) -> None:
        """Reject (nack) a message from the queue."""
        if not self.subscriber:
            raise RuntimeError("subscriber is not connected")

        logging.debug(log_msgs.NACKING_MESSAGE)
        # TODO
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
        if not self.subscriber:
            raise RuntimeError("subscriber is not connected")

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

    # NOTE - making unique subscription ids would create independent (but identical?) queues
    # See https://thecloudgirl.dev/images/pubsub.jpg
    SUBSCRIPTION_ID = "i3-gcp-sub"

    @staticmethod
    def create_pub_queue(address: str, name: str) -> GCPPub:
        """Create a publishing queue."""
        q = GCPPub(address, Backend.PROJECT_ID, name)  # pylint: disable=invalid-name
        q.connect()
        return q

    @staticmethod
    def create_sub_queue(address: str, name: str, prefetch: int = 1) -> GCPSub:
        """Create a subscription queue."""
        # pylint: disable=invalid-name
        q = GCPSub(address, Backend.PROJECT_ID, name, Backend.SUBSCRIPTION_ID)
        # q.prefetch = prefetch
        q.connect()
        return q
