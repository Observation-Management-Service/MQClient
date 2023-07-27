"""Back-end using Apache Pulsar."""

import asyncio
import functools
import logging
import os
from typing import AsyncGenerator, Optional

import pulsar  # type: ignore

from .. import broker_client_interface, log_msgs
from ..broker_client_interface import (
    ClosingFailedException,
    Message,
    MQClientException,
    Pub,
    RawQueue,
    Sub,
)
from . import utils

LOGGER = logging.getLogger("mqclient.pulsar")


class Pulsar(RawQueue):
    """Base Pulsar wrapper.

    Extends:
        RawQueue
    """

    def __init__(
        self,
        address: str,
        topic: str,
        auth_token: str,
    ) -> None:
        """Set address, topic, and client.

        Arguments:
            address {str} -- the pulsar server address, if address doesn't start with 'pulsar', append 'pulsar://'
            topic {str} -- the name of the topic
            auth_token {str} -- the (jwt) authentication token
        """
        super().__init__()
        LOGGER.info(f"Requested MQClient for topic '{topic}' @ {address}")

        self.address = address
        if not self.address.startswith("pulsar"):
            self.address = "pulsar://" + self.address
        self.topic = topic
        self.client: pulsar.Client = None
        self.auth = pulsar.AuthenticationToken(auth_token) if auth_token else None
        self._auth_token = auth_token

    async def connect(self) -> None:
        """Set up client."""
        await super().connect()
        self.client = pulsar.Client(self.address, authentication=self.auth)

    async def close(self) -> None:
        """Close client."""
        await super().close()
        if not self.client:
            raise ClosingFailedException("No client to close.")
        try:
            self.client.close()
        except Exception as e:
            # https://github.com/apache/pulsar/issues/3127
            if str(e) == "Pulsar error: AlreadyClosed":
                LOGGER.warning("Attempted to close a connection that is already closed")
                return
            raise ClosingFailedException(str(e)) from e


class PulsarPub(Pulsar, Pub):
    """Wrapper around pulsar.Producer.

    Extends:
        Pulsar
        Pub
    """

    def __init__(
        self,
        address: str,
        topic: str,
        auth_token: str,
    ) -> None:
        LOGGER.debug(f"{log_msgs.INIT_PUB} ({address}; {topic})")
        super().__init__(address, topic, auth_token)
        self.producer: pulsar.Producer = None

    async def connect(self) -> None:
        """Connect to producer."""
        LOGGER.debug(log_msgs.CONNECTING_PUB)
        await super().connect()

        # create sub so that subscription is created so messages are forwarded from topic
        # https://pulsar.apache.org/assets/images/pulsar-subscription-types-664733b68c7124129ca7d0e04dedcb96.png
        inner_sub = PulsarSub(
            self.address,
            self.topic,
            BrokerClient.SUBSCRIPTION_NAME,
            self._auth_token,
            prefetch=1,
        )
        await inner_sub.connect()
        await inner_sub.close()

        self.producer = self.client.create_producer(self.topic)
        LOGGER.debug(log_msgs.CONNECTED_PUB)

    async def close(self) -> None:
        """Close connection."""
        LOGGER.debug(log_msgs.CLOSING_PUB)
        await super().close()
        if not self.producer:
            raise ClosingFailedException("No producer to sub.")
        LOGGER.debug(log_msgs.CLOSED_PUB)

    async def send_message(
        self,
        msg: bytes,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Send a message on a queue."""
        LOGGER.debug(log_msgs.SENDING_MESSAGE)
        if not self.producer:
            raise MQClientException("queue is not connected")

        def _send_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self.producer:
                raise MQClientException("queue is not connected")
            return self.producer.send(msg)

        await utils.auto_retry_call(
            func=_send_msg,
            retries=retries,
            retry_delay=retry_delay,
            close=(
                None
                if self.connection_can_have_multiple_unacked_messages
                else self.close
            ),
            connect=(
                None
                if self.connection_can_have_multiple_unacked_messages
                else self.connect
            ),
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(log_msgs.SENT_MESSAGE)


class PulsarSub(Pulsar, Sub):
    """Wrapper around pulsar.Consumer.

    Extends:
        Pulsar
        Sub
    """

    def __init__(
        self,
        address: str,
        topic: str,
        subscription_name: str,
        auth_token: str,
        prefetch: int,
    ) -> None:
        LOGGER.debug(f"{log_msgs.INIT_SUB} ({address}; {topic})")
        super().__init__(address, topic, auth_token)
        self.consumer: pulsar.Consumer = None
        self.subscription_name = subscription_name
        self.prefetch = prefetch

    async def connect(self) -> None:
        """Connect to subscriber."""
        LOGGER.debug(log_msgs.CONNECTING_SUB)
        await super().connect()

        ack_timeout: Optional[int]  # for mypy
        try:
            ack_timeout = int(os.environ["PULSAR_UNACKED_MESSAGES_TIMEOUT_SEC"])
            if ack_timeout < 10:
                ack_timeout = None
        except:  # noqa: E722
            ack_timeout = None

        self.consumer = self.client.subscribe(
            self.topic,
            self.subscription_name,
            # Neither receive with timeout nor partitioned topics can be used if the consumer queue size is zero.
            receiver_queue_size=max(self.prefetch, 1),
            unacked_messages_timeout_ms=ack_timeout,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            negative_ack_redelivery_delay_ms=0,
        )
        LOGGER.debug(log_msgs.CONNECTED_SUB)

    async def close(self) -> None:
        """Close client and redeliver any unacknowledged messages."""
        LOGGER.debug(log_msgs.CLOSING_SUB)
        if not self.consumer:
            raise ClosingFailedException("No consumer to close.")
        await asyncio.sleep(0.1)
        self.consumer.redeliver_unacknowledged_messages()
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: pulsar.Message,
    ) -> Optional[Message]:
        """Transform Puslar-Message to Message type."""
        id_, data = msg.message_id(), msg.data()

        if id_ is None or data is None:  # message_id may be 0; data may be b''
            return None

        # Need to serialize id? (message_id.serialize() -> bytes)
        if isinstance(id_, pulsar._pulsar.MessageId):  # pylint: disable=I1101,W0212
            return Message(id_.serialize(), data)
        # Send original data
        else:
            return Message(id_, data)

    async def get_message(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: float,
    ) -> Optional[Message]:
        """Get a single message from a queue.

        To endlessly block until a message is available, set
        `timeout_millis=None`.
        """
        LOGGER.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self.consumer:
            raise MQClientException("queue is not connected")

        def _get_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self.consumer:
                raise MQClientException("queue is not connected")
            return self.consumer.receive(timeout_millis=timeout_millis)

        try:
            pulsar_msg = await utils.auto_retry_call(
                func=_get_msg,
                retries=retries,
                retry_delay=retry_delay,
                close=(
                    None
                    if self.connection_can_have_multiple_unacked_messages
                    else self.close
                ),
                connect=(
                    None
                    if self.connection_can_have_multiple_unacked_messages
                    else self.connect
                ),
                logger=LOGGER,
                nonretriable_conditions=lambda e: str(e) == "Pulsar error: TimeOut",
            )
        except Exception as e:
            # https://github.com/apache/pulsar/issues/3127
            # consumer timed out so there's nothing left in the tube
            if str(e) == "Pulsar error: TimeOut":
                LOGGER.debug(log_msgs.GETMSG_TIMEOUT_ERROR)
                return None
            raise
        if msg := PulsarSub._to_message(pulsar_msg):
            LOGGER.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg}).")
            return msg
        else:
            LOGGER.debug(log_msgs.GETMSG_NO_MESSAGE)
            return None

    async def ack_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Ack a message from the queue."""
        LOGGER.debug(log_msgs.ACKING_MESSAGE)
        if not self.consumer:
            raise MQClientException("queue is not connected")

        if isinstance(msg.msg_id, bytes):
            pulsar_msg = pulsar.MessageId.deserialize(msg.msg_id)
        else:
            pulsar_msg = msg.msg_id

        await utils.auto_retry_call(
            func=functools.partial(
                self.consumer.acknowledge,
                pulsar_msg,
            ),
            retries=retries,
            retry_delay=retry_delay,
            close=None,
            connect=None,
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.ACKED_MESSAGE} ({msg}).")

    async def reject_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Reject (nack) a message from the queue."""
        LOGGER.debug(log_msgs.NACKING_MESSAGE)
        if not self.consumer:
            raise MQClientException("queue is not connected")

        if isinstance(msg.msg_id, bytes):
            pulsar_msg = pulsar.MessageId.deserialize(msg.msg_id)
        else:
            pulsar_msg = msg.msg_id

        await utils.auto_retry_call(
            func=functools.partial(
                self.consumer.negative_acknowledge,
                pulsar_msg,
            ),
            retries=retries,
            retry_delay=retry_delay,
            close=None,
            connect=None,
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.NACKED_MESSAGE} ({msg}).")

    async def message_generator(
        self,
        timeout: int,
        propagate_error: bool,
        retries: int,
        retry_delay: float,
    ) -> AsyncGenerator[Optional[Message], None]:
        """Yield Messages.

        Generate messages with variable timeout.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        LOGGER.debug(log_msgs.MSGGEN_ENTERED)
        if not self.consumer:
            raise MQClientException("queue is not connected")

        msg = None
        try:
            while True:
                # get message
                LOGGER.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = await self.get_message(
                    timeout_millis=timeout * 1000,
                    retries=retries,
                    retry_delay=retry_delay,
                )
                if msg is None:
                    LOGGER.info(log_msgs.MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE)
                    break

                # yield message to consumer
                try:
                    LOGGER.debug(f"{log_msgs.MSGGEN_YIELDING_MESSAGE} [{msg}]")
                    yield msg
                # consumer throws Exception...
                except Exception as e:  # pylint: disable=W0703
                    LOGGER.debug(log_msgs.MSGGEN_DOWNSTREAM_ERROR)
                    if propagate_error:
                        LOGGER.debug(log_msgs.MSGGEN_PROPAGATING_ERROR)
                        raise
                    LOGGER.warning(
                        f"{log_msgs.MSGGEN_EXCEPTED_DOWNSTREAM_ERROR} {e}.",
                        exc_info=True,
                    )
                    yield None  # hand back to consumer
                # consumer requests again, aka next()
                else:
                    pass

        # garbage collection (or explicit generator close(), or break in consumer's loop)
        except GeneratorExit:
            LOGGER.debug(log_msgs.MSGGEN_GENERATOR_EXITING)
            LOGGER.debug(log_msgs.MSGGEN_GENERATOR_EXITED)


class BrokerClient(broker_client_interface.BrokerClient):
    """Pulsar Pub-Sub BrokerClient Factory.

    Extends:
        BrokerClient
    """

    NAME = "pulsar"

    # NOTE - use single shared subscription
    # (making multiple unique subscription names would create independent subscriptions)
    SUBSCRIPTION_NAME = "i3-pulsar-sub"

    @staticmethod
    async def create_pub_queue(
        address: str,
        name: str,
        auth_token: str,
    ) -> PulsarPub:
        """Create a publishing queue."""
        q = PulsarPub(  # pylint: disable=invalid-name
            address,
            name,
            auth_token,
        )
        await q.connect()
        return q

    @staticmethod
    async def create_sub_queue(
        address: str,
        name: str,
        prefetch: int,
        auth_token: str,
    ) -> PulsarSub:
        """Create a subscription queue."""
        q = PulsarSub(  # pylint: disable=invalid-name
            address,
            name,
            BrokerClient.SUBSCRIPTION_NAME,
            auth_token,
            prefetch,
        )
        await q.connect()
        return q
