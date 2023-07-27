"""Back-end using NATS."""


import logging
import math
from typing import Any, AsyncGenerator, List, Optional, TypeVar, cast

import nats

from .. import broker_client_interface, log_msgs
from ..broker_client_interface import (
    ClosingFailedException,
    Message,
    MQClientException,
    Pub,
    RawQueue,
    Sub,
)
from ..config import DEFAULT_TIMEOUT_MILLIS
from . import utils

LOGGER = logging.getLogger("mqclient.nats")

T = TypeVar("T")  # the callable/awaitable return type


async def _anext(gen: AsyncGenerator[Any, Any], default: Any) -> Any:
    """Provide the functionality of python 3.10's `anext()`.

    https://docs.python.org/3/library/functions.html#anext
    """
    try:
        return await gen.__anext__()
    except StopAsyncIteration:
        return default


class NATS(RawQueue):
    """Base NATS wrapper, using JetStream.

    Extends:
        RawQueue
    """

    def __init__(self, endpoint: str, stream_id: str, subject: str) -> None:
        super().__init__()
        LOGGER.info(
            f"Requested MQClient for stream_id/subject '{stream_id}/{subject}' @ {endpoint}"
        )

        self.endpoint = endpoint
        self.subject = subject
        self.stream_id = stream_id

        self._nats_client: Optional[nats.aio.client.Client] = None
        self.js: Optional[nats.js.JetStreamContext] = None

        LOGGER.debug(f"Stream & Subject: {stream_id}/{self.subject}")

    async def connect(self) -> None:
        """Set up connection and channel."""
        await super().connect()
        self._nats_client = await nats.connect(self.endpoint)  # type: ignore[arg-type]
        # Create JetStream context
        self.js = self._nats_client.jetstream(timeout=DEFAULT_TIMEOUT_MILLIS // 1000)
        await self.js.add_stream(name=self.stream_id, subjects=[self.subject])

    async def close(self) -> None:
        """Close connection."""
        await super().close()
        if not self._nats_client:
            raise ClosingFailedException("No connection to close.")
        await self._nats_client.close()


class NATSPub(NATS, Pub):
    """Wrapper around PublisherClient, using JetStream.

    Extends:
        NATS
        Pub
    """

    def __init__(self, endpoint: str, stream_id: str, subject: str):
        LOGGER.debug(f"{log_msgs.INIT_PUB} ({endpoint}; {stream_id}; {subject})")
        super().__init__(endpoint, stream_id, subject)
        # NATS is pub-centric, so no extra instance needed

    async def connect(self) -> None:
        """Set up pub, then create topic and any subscriptions indicated."""
        LOGGER.debug(log_msgs.CONNECTING_PUB)
        await super().connect()
        LOGGER.debug(log_msgs.CONNECTED_PUB)

    async def close(self) -> None:
        """Close pub (no-op)."""
        LOGGER.debug(log_msgs.CLOSING_PUB)
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_PUB)

    async def send_message(
        self,
        msg: bytes,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Send a message (publish)."""
        LOGGER.debug(log_msgs.SENDING_MESSAGE)
        if not self.js:
            raise MQClientException("JetStream is not connected")

        async def _send_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self.js:
                raise MQClientException("JetStream is not connected")
            return await self.js.publish(self.subject, msg)

        ack: nats.js.api.PubAck = await utils.auto_retry_call(
            func=_send_msg,
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(f"Sent Message w/ Ack: {ack}")
        LOGGER.debug(log_msgs.SENT_MESSAGE)


class NATSSub(NATS, Sub):
    """Wrapper around queue with prefetch-queue, using JetStream.

    Extends:
        NATS
        Sub
    """

    def __init__(
        self,
        endpoint: str,
        stream_id: str,
        subject: str,
        prefetch: int,
    ):
        LOGGER.debug(f"{log_msgs.INIT_SUB} ({endpoint}; {stream_id}; {subject})")
        super().__init__(endpoint, stream_id, subject)
        self._subscription: Optional[nats.js.JetStreamContext.PullSubscription] = None
        self.prefetch = prefetch

    async def connect(self) -> None:
        """Set up sub (pull subscription)."""
        LOGGER.debug(log_msgs.CONNECTING_SUB)
        await super().connect()
        if not self.js:
            raise MQClientException("JetStream is not connected.")

        self._subscription = await self.js.pull_subscribe(self.subject, "psub")
        LOGGER.debug(log_msgs.CONNECTED_SUB)

    async def close(self) -> None:
        """Close sub."""
        LOGGER.debug(log_msgs.CLOSING_SUB)
        if not self._subscription:
            raise ClosingFailedException("No sub to close.")
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        msg: nats.aio.msg.Msg,  # pylint: disable=no-member
    ) -> Optional[Message]:
        """Transform NATS-Message to Message type."""
        return Message(msg.reply, msg.data)

    def _from_message(self, msg: Message) -> nats.aio.msg.Msg:
        """Transform Message instance to NATS-Message.

        Assumes the message came from this NATSSub instance.
        """
        if not self._nats_client:
            raise MQClientException("Client is not connected")

        return nats.aio.msg.Msg(
            _client=self._nats_client,
            subject=self.subject,
            reply=cast(str, msg.msg_id),  # we know this is str b/c `_to_message()`
            data=msg.data,
            headers=None,  # default
        )

    async def _get_messages(
        self,
        timeout_millis: Optional[int],
        num_messages: int,
        retries: int,
        retry_delay: float,
    ) -> List[Message]:
        """Get n messages.

        The subscriber pulls a specific number of messages. The actual
        number of messages pulled may be smaller than `num_messages`.
        """
        if not self._subscription:
            raise MQClientException("Subscriber is not connected")

        if not timeout_millis:
            timeout_millis = DEFAULT_TIMEOUT_MILLIS

        async def _get_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self._subscription:
                raise MQClientException("Subscriber is not connected")
            return await self._subscription.fetch(
                batch=num_messages,
                timeout=int(math.ceil(timeout_millis / 1000)),
            )

        try:
            nats_msgs: List[nats.aio.msg.Msg] = await utils.auto_retry_call(
                func=_get_msg,
                retries=retries,
                retry_delay=retry_delay,
                close=self.close,
                connect=self.connect,
                logger=LOGGER,
                nonretriable_conditions=lambda e: isinstance(
                    e, nats.errors.TimeoutError
                ),
            )
        except nats.errors.TimeoutError:
            LOGGER.debug(log_msgs.GETMSG_TIMEOUT_ERROR)
            return []
        if not nats_msgs:
            LOGGER.debug(log_msgs.GETMSG_NO_MESSAGE)
            return []
        msgs = []
        for recvd in nats_msgs:
            if msg := self._to_message(recvd):
                LOGGER.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg}).")
                msgs.append(msg)
        return msgs

    async def get_message(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: float,
    ) -> Optional[Message]:
        """Get a message."""
        LOGGER.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self._subscription:
            raise MQClientException("Subscriber is not connected.")

        try:
            msg = (
                await self._get_messages(
                    timeout_millis,
                    1,
                    retries,
                    retry_delay,
                )
            )[0]
            return msg
        except IndexError:
            return None

    async def _gen_messages(
        self,
        timeout_millis: Optional[int],
        num_messages: int,
        retries: int,
        retry_delay: float,
    ) -> AsyncGenerator[Message, None]:
        """Continuously generate messages until there are no more."""
        if not self._subscription:
            raise MQClientException("Subscriber is not connected.")

        while True:
            msgs = await self._get_messages(
                timeout_millis,
                num_messages,
                retries,
                retry_delay,
            )
            if not msgs:
                return
            for msg in msgs:
                yield msg

    async def ack_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Ack a message from the queue."""
        LOGGER.debug(log_msgs.ACKING_MESSAGE)
        if not self._subscription:
            raise MQClientException("subscriber is not connected")

        async def _ack_msg():
            # use wrapper function so connection references can be updated by reconnects
            return await self._from_message(msg).ack()

        # Acknowledges the received messages so they will not be sent again.
        await utils.auto_retry_call(
            func=_ack_msg,
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")

    async def reject_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Reject (nack) a message from the queue."""
        LOGGER.debug(log_msgs.NACKING_MESSAGE)
        if not self._subscription:
            raise MQClientException("subscriber is not connected")

        async def _nack_msg():
            # use wrapper function so connection references can be updated by reconnects
            return await self._from_message(msg).nak()  # yes, it's "nak"

        await utils.auto_retry_call(
            func=_nack_msg,
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            nonretriable_conditions=None,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.NACKED_MESSAGE} ({msg.msg_id!r}).")

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
        if not self._subscription:
            raise MQClientException("subscriber is not connected")

        msg = None
        try:
            gen = self._gen_messages(
                timeout * 1000,
                self.prefetch,  # prefetch = # of msgs pulled
                retries,
                retry_delay,
            )
            while True:
                # get message
                LOGGER.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                msg = await _anext(gen, None)
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
    """NATS Pub-Sub BrokerClient Factory.

    Extends:
        BrokerClient
    """

    NAME = "nats"

    @staticmethod
    async def create_pub_queue(
        address: str,
        name: str,
        auth_token: str,
    ) -> NATSPub:
        """Create a publishing queue.

        # NOTE - `auth_token` is not used currently
        """
        if auth_token:
            LOGGER.warning("NATS broker client does not use 'auth_token'")

        q = NATSPub(  # pylint: disable=invalid-name
            address,
            name + "-stream",
            name + "-subject",
        )
        await q.connect()
        return q

    @staticmethod
    async def create_sub_queue(
        address: str,
        name: str,
        prefetch: int,
        auth_token: str,
    ) -> NATSSub:
        """Create a subscription queue.

        # NOTE - `auth_token` is not used currently
        """
        if auth_token:
            LOGGER.warning("NATS broker client does not use 'auth_token'")

        q = NATSSub(  # pylint: disable=invalid-name
            address,
            name + "-stream",
            name + "-subject",
            prefetch,
        )
        await q.connect()
        return q
