"""Back-end using RabbitMQ."""

import functools
import logging
import urllib
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import pika  # type: ignore

from .. import broker_client_interface, log_msgs
from ..broker_client_interface import (
    ClosingFailedException,
    ConnectingFailedException,
    Message,
    MQClientException,
    Pub,
    RawQueue,
    Sub,
)
from . import utils

StrDict = Dict[str, Any]

LOGGER = logging.getLogger("mqclient.rabbitmq")

HEARTBEAT_STREAMLOSTERROR_MSG = (
    "pika.exceptions.StreamLostError: may be due to a missed heartbeat"
)


HUMAN_PATTERN = "[SCHEME://][USER[:PASS]@]HOST[:PORT][/VIRTUAL_HOST]"


def _parse_url(url: str) -> Tuple[StrDict, Optional[str], Optional[str]]:
    if "://" not in url:
        url = "//" + url
    result = urllib.parse.urlparse(url)

    parts = dict(
        scheme=result.scheme,
        host=result.hostname,
        port=result.port,
        virtual_host=result.path.lstrip("/"),
    )
    # for putting into ConnectionParameters filter ""/None (will rely on defaults)
    parts = {k: v for k, v in parts.items() if v}  # host=..., etc.

    # check validity
    if not parts or "host" not in parts:
        raise MQClientException(f"Invalid address: {url} (format: {HUMAN_PATTERN})")

    return parts, result.username, result.password


def _get_credentials(
    username: Optional[str], password: Optional[str], auth_token: str
) -> Optional[pika.credentials.PlainCredentials]:
    if auth_token:
        password = auth_token

    # Case 1: username/password
    if username and password:
        return pika.credentials.PlainCredentials(username, password)
    # Case 2: Only password/token -- Ex: keycloak
    elif (not username) and password:
        return pika.credentials.PlainCredentials("", password)
    # Error: no password for user
    elif username and (not password):
        raise MQClientException("username given but no password or token")
    # Case 3: no auth -- rabbitmq uses guest/guest
    else:  # not username and not password
        return None


class RabbitMQ(RawQueue):
    """Base RabbitMQ wrapper.

    Extends:
        RawQueue
    """

    def __init__(
        self,
        address: str,
        queue: str,
        auth_token: str,
    ) -> None:
        super().__init__()
        LOGGER.info(f"Requested MQClient for queue '{queue}' @ {address}")
        cp_args, _user, _pass = _parse_url(address)

        # set up connection parameters
        if creds := _get_credentials(_user, _pass, auth_token):
            cp_args["credentials"] = creds

        self.parameters = pika.connection.ConnectionParameters(**cp_args)

        self.queue = queue
        self.connection: Optional[pika.BlockingConnection] = None
        self._next_channel_number = 1  # must start at 1 (not 0)

    def open_channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Add a channel for the connection and configure."""
        LOGGER.info(f"Adding channel to connection for '{self.queue=}'")
        if not self.connection:
            raise ClosingFailedException("No connection to add channel.")

        # give unique channel_number b/c pika has a delay on re-connections in which it will recycle a closed channel
        channel = self.connection.channel(self._next_channel_number)
        # alternatively, we could use: int(uuid.uuid4()) % pika.channel.MAX_CHANNELS + 1
        # the channel number gets put in a struct and it's constrained to an unsigned short
        # 0 is not allowed and will be treated as None
        self._next_channel_number += 1

        """
        We need to discuss how many RabbitMQ instances we want to run
        the default is that the quorum queue is spread across 3 nodes
        so 1 can fail without issue. Maybe we want to up this for
        more production workloads
        """
        channel.queue_declare(
            queue=self.queue, durable=True, arguments={"x-queue-type": "quorum"}
        )

        # self.channels.append(channel)
        LOGGER.info(f"Opened channel '{channel.channel_number}'")
        return channel

    async def connect(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Set up connection and open 1 channel."""
        await super().connect()
        LOGGER.info(f"Connecting with parameters={self.parameters}")

        self.connection = pika.BlockingConnection(self.parameters)
        channel = self.open_channel()
        if not channel:
            raise ConnectingFailedException("Channel was not connected")
        return channel

    async def close(self) -> None:
        """Close connection."""
        await super().close()

        # if not self.channels:
        #     raise ClosingFailedException("No channel to close.")
        if not self.connection:
            raise ClosingFailedException("No connection to close.")
        if self.connection.is_closed:
            LOGGER.warning("Attempted to close a connection that is already closed")
            return

        try:
            # self.channel.cancel() -- done by self.connection.close()
            self.connection.close()
        except Exception as e:
            raise ClosingFailedException() from e


class RabbitMQPub(RabbitMQ, Pub):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        RabbitMQ
        Pub
    """

    def __init__(
        self,
        address: str,
        name: str,
        auth_token: str,
    ) -> None:
        LOGGER.debug(f"{log_msgs.INIT_PUB} ({address}; {name})")
        super().__init__(address, name, auth_token)
        self.channel: pika.adapters.blocking_connection.BlockingChannel = []

    def open_channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Add a channel for the connection and configure."""
        if self.channel:
            raise MQClientException("RabbitMQPub instance can only have one channel")

        channel = super().open_channel()
        channel.confirm_delivery()
        return channel

    async def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        LOGGER.debug(log_msgs.CONNECTING_PUB)
        self.channel = await super().connect()
        LOGGER.debug(log_msgs.CONNECTED_PUB)

    async def close(self) -> None:
        """Close connection."""
        LOGGER.debug(log_msgs.CLOSING_PUB)
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_PUB)

    async def send_message(
        self,
        msg: bytes,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Send a message on a queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        LOGGER.debug(log_msgs.SENDING_MESSAGE)
        if not self.channel:
            raise MQClientException("queue is not connected")

        def _send_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self.channel:
                raise MQClientException("queue is not connected")
            LOGGER.debug(f"sending on channel: {self.channel.channel_number}")
            return self.channel.basic_publish(
                exchange="",
                routing_key=self.queue,
                body=msg,
            )

        try:
            await utils.auto_retry_call(
                func=_send_msg,
                nonretriable_conditions=lambda e: isinstance(
                    e,
                    (pika.exceptions.AMQPChannelError, pika.exceptions.StreamLostError),
                ),
                retries=retries,
                retry_delay=retry_delay,
                close=self.close,
                connect=self.connect,
                logger=LOGGER,
            )
            LOGGER.debug(log_msgs.SENT_MESSAGE)
        except pika.exceptions.StreamLostError as e:
            raise MQClientException(HEARTBEAT_STREAMLOSTERROR_MSG) from e


class RabbitMQSub(RabbitMQ, Sub):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        RabbitMQ
        Sub
    """

    def __init__(
        self,
        address: str,
        name: str,
        auth_token: str,
        prefetch: int,
    ) -> None:
        LOGGER.debug(f"{log_msgs.INIT_SUB} ({address}; {name})")
        super().__init__(address, name, auth_token)
        self.consumer_id = None
        self.prefetch = prefetch

        self.active_channels: List[
            pika.adapters.blocking_connection.BlockingChannel
        ] = []
        self.reserve_channel: Optional[
            pika.adapters.blocking_connection.BlockingChannel
        ] = None

    def open_channel(self) -> pika.adapters.blocking_connection.BlockingChannel:
        """Add a channel for the connection and configure.

        Turn on prefetching.
        """
        channel = super().open_channel()

        channel.basic_qos(prefetch_count=max(self.prefetch, 1))
        # Setting the value to 0 lets the consumer drain the entire queue.
        # https://www.cloudamqp.com/blog/rabbitmq-basic-consume-vs-rabbitmq-basic-get.html#what-are-the-advantages-of-a-rabbitmq-consumer
        # https://www.cloudamqp.com/blog/part1-rabbitmq-best-practice.html#prefetch
        #
        # https://www.rabbitmq.com/consumer-prefetch.html
        #    Meaning of prefetch_count in RabbitMQ w/ global_qos=False:
        #    applied separately to each new consumer on the channel
        #
        # global_qos=False b/c using quorum queues
        # https://www.rabbitmq.com/quorum-queues.html#global-qos

        return channel

    async def connect(self) -> None:
        """Set up connection, channel, and queue."""
        LOGGER.debug(log_msgs.CONNECTING_SUB)
        channel = await super().connect()
        self.active_channels.append(channel)
        LOGGER.debug(log_msgs.CONNECTED_SUB)

    async def close(self) -> None:
        """Close connection.

        Also, channel will be canceled (rejects all pending ackable
        messages).
        """
        LOGGER.debug(log_msgs.CLOSING_SUB)
        await super().close()
        for channel in self.active_channels + [self.reserve_channel]:
            if channel.is_open:
                LOGGER.warning(
                    f"Channel remains open after connection close: {channel.channel_number}."
                )
        self.active_channels = []
        self.reserve_channel = None
        LOGGER.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        method_frame: Optional[pika.spec.Basic.GetOk],
        body: Optional[Union[str, bytes]],
        channel_number: int,
    ) -> Optional[Message]:
        """Transform RabbitMQ-Message to Message type."""
        if not method_frame or body is None:
            return None

        if isinstance(body, str):
            msg = Message(method_frame.delivery_tag, body.encode())
        else:
            msg = Message(method_frame.delivery_tag, body)

        msg._connection_id = channel_number
        return msg

    async def _iter_messages(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: float,
    ) -> AsyncIterator[Optional[Message]]:
        if not self.active_channels:
            raise MQClientException("queue is not connected")

        def _get_msg():
            # use wrapper function so connection references can be updated by reconnects
            if not self.active_channels:
                raise MQClientException("queue is not connected")
            LOGGER.debug(f"consuming on channel: {channel.channel_number}")
            try:
                return next(
                    # pika smartly handles re-invocations
                    channel.consume(
                        self.queue,
                        inactivity_timeout=timeout_millis / 1000.0
                        if timeout_millis
                        else None,
                    )
                )
            except StopIteration:
                return (None, None, None)

        # def infinite_loop_over_channels() -> (
        #     Iterator[pika.adapters.blocking_connection.BlockingChannel]
        # ):
        #     # this allows self.channels to be updated,
        #     # updates are reflected on outer-loop
        #     # itertools.cycle() does not allow updates
        #     while True:
        #         yield from self.channels

        # inf_channels_gen = infinite_loop_over_channels()
        # channel = next(inf_channels_gen)  # always called manually
        # n_nonempty_channels_remaining = len(self.channels)  # assume all are non-empty
        remaining_channels = [c for c in self.active_channels]  # start with all
        channel = remaining_channels[0]  # TODO - use by priority?

        while True:
            try:
                pika_msg = await utils.auto_retry_call(
                    func=_get_msg,
                    nonretriable_conditions=lambda e: isinstance(
                        e,
                        (
                            pika.exceptions.AMQPChannelError,
                            pika.exceptions.StreamLostError,
                        ),
                    ),
                    retries=retries,
                    retry_delay=retry_delay,
                    close=None,
                    connect=None,
                    logger=LOGGER,
                )
            except pika.exceptions.StreamLostError as e:
                raise MQClientException(HEARTBEAT_STREAMLOSTERROR_MSG) from e

            # YIELD (got a message)
            if msg := RabbitMQSub._to_message(
                pika_msg[0],
                pika_msg[2],
                channel.channel_number,
            ):
                LOGGER.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg.msg_id!r}).")
                # n_nonempty_channels_remaining = len(self.channels)  # reset!
                if channel == self.reserve_channel:
                    # if this was the reserve channel, move it to active channels
                    self.active_channels.append(self.reserve_channel)
                    self.reserve_channel = None  # no need to open a new one now
                remaining_channels = [c for c in self.active_channels]  # reset!
                yield msg
            # DEAL WITH EMPTY CHANNEL (didn't get a message)
            else:
                # n_nonempty_channels_remaining -= 1
                LOGGER.debug("No message received -- switching channels...")
                if channel == self.reserve_channel:
                    # this means our reserve channel came up empty,
                    # so there's REALLY nothing in the queue
                    LOGGER.debug(log_msgs.GETMSG_NO_MESSAGE)
                    remaining_channels = [c for c in self.active_channels]  # reset!
                    yield None
                else:
                    remaining_channels.remove(channel)
                    # if n_nonempty_channels_remaining == 0:
                    if not remaining_channels:
                        # don't reset n_nonempty_channels_remaining so we can see if this one is empty
                        # channel = self.open_channel()  # try it now
                        # this new channel will be yielded by inf_channels_gen eventually

                        # FIXME - this leads to MANY empty channels if the user
                        #   keeps cycling despite coming up empty
                        # TODO - figure way to close or limit channels, but
                        #   consider that some ack-pending messages need to use
                        #   the channel to ack. Keep one local mapping?
                        #   Currently uses msg._connection_id.
                        #   Maybe just closing a newly-opened channel will suffice?

                        # try reserve channel
                        if not self.reserve_channel:
                            self.reserve_channel = self.open_channel()
                        channel = self.reserve_channel

                        # continue
                    # elif n_nonempty_channels_remaining < 0:  # -1
                    #     # this means our newly produced channel is empty,
                    #     # so there's REALLY nothing in the queue
                    #     LOGGER.debug(log_msgs.GETMSG_NO_MESSAGE)
                    #     self.reserve_channel
                    #     channel = next(inf_channels_gen)  # try next, next time
                    #     # FIXME - this design still allows a lot of new channels, one per final iter...
                    #     yield None
                    else:
                        # channel = next(inf_channels_gen)  # try next, now
                        channel = remaining_channels[0]  # TODO - use by priority?
                        # continue

    async def get_message(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: float,
    ) -> Optional[Message]:
        """Get a message from a queue."""
        LOGGER.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self.active_channels:
            raise MQClientException("queue is not connected")

        msg = None
        async for msg in self._iter_messages(timeout_millis, retries, retry_delay):
            # None -> timeout
            break  # get just one message

        # self.channel.cancel()  # this is done by `open_sub_one()` *after* ack/nack via `close()`

        return msg

    def _get_channel_by_msg(
        self, msg: Message
    ) -> pika.adapters.blocking_connection.BlockingChannel:
        """Map message to channel."""
        matches = [
            c for c in self.active_channels if c.channel_number == msg._connection_id
        ]
        if not matches:
            raise MQClientException(
                f"could not map message to channel: {msg} {self.active_channels}"
            )
        elif len(matches) > 1:
            raise MQClientException(
                f"message mapped to multiple channels: {msg} {matches}"
            )
        else:
            return matches[0]

    async def ack_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Ack a message from the queue."""
        LOGGER.debug(log_msgs.ACKING_MESSAGE)
        if not self.active_channels:
            raise MQClientException("queue is not connected")

        channel = self._get_channel_by_msg(msg)
        LOGGER.debug(f"acking on channel: {channel.channel_number}")

        try:
            await utils.auto_retry_call(
                func=functools.partial(
                    channel.basic_ack,
                    msg.msg_id,
                    multiple=False,
                ),
                connect=None,
                close=None,
                nonretriable_conditions=lambda e: isinstance(
                    e,
                    (pika.exceptions.AMQPChannelError, pika.exceptions.StreamLostError),
                ),
                retries=retries,
                retry_delay=retry_delay,
                logger=LOGGER,
            )
            LOGGER.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")
        except pika.exceptions.StreamLostError as e:
            raise MQClientException(HEARTBEAT_STREAMLOSTERROR_MSG) from e

    async def reject_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Reject (nack) a message from the queue."""
        LOGGER.debug(log_msgs.NACKING_MESSAGE)
        if not self.active_channels:
            raise MQClientException("queue is not connected")

        channel = self._get_channel_by_msg(msg)
        LOGGER.debug(f"nacking on channel: {channel.channel_number}")

        try:
            await utils.auto_retry_call(
                func=functools.partial(
                    channel.basic_nack,
                    msg.msg_id,
                    multiple=False,
                    requeue=True,
                ),
                close=None,
                connect=None,
                nonretriable_conditions=lambda e: isinstance(
                    e,
                    (pika.exceptions.AMQPChannelError, pika.exceptions.StreamLostError),
                ),
                retries=retries,
                retry_delay=retry_delay,
                logger=LOGGER,
            )
            LOGGER.debug(f"{log_msgs.NACKED_MESSAGE} ({msg.msg_id!r}).")
        except pika.exceptions.StreamLostError as e:
            raise MQClientException(HEARTBEAT_STREAMLOSTERROR_MSG) from e

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
            propagate_error -- should errors from downstream kill the generator? (default: {True})
        """
        LOGGER.debug(log_msgs.MSGGEN_ENTERED)
        if not self.active_channels:
            raise MQClientException("queue is not connected")

        msg = None
        try:
            async for msg in self._iter_messages(timeout * 1000, retries, retry_delay):
                LOGGER.debug(log_msgs.MSGGEN_GET_NEW_MESSAGE)
                if not msg:
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

        # Garbage Collection (or explicit generator close(), or break in consumer's loop)
        except GeneratorExit:
            LOGGER.debug(log_msgs.MSGGEN_GENERATOR_EXITING)
            LOGGER.debug(log_msgs.MSGGEN_GENERATOR_EXITED)

        # Done with generator, one way or another
        finally:
            pass


class BrokerClient(broker_client_interface.BrokerClient):
    """RabbitMQ Pub-Sub BrokerClient Factory.

    Extends:
        BrokerClient
    """

    NAME = "rabbitmq"

    @staticmethod
    async def create_pub_queue(
        address: str,
        name: str,
        auth_token: str,
    ) -> RabbitMQPub:
        """Create a publishing queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        # pylint: disable=invalid-name
        q = RabbitMQPub(address, name, auth_token)
        await q.connect()
        return q

    @staticmethod
    async def create_sub_queue(
        address: str,
        name: str,
        prefetch: int,
        auth_token: str,
    ) -> RabbitMQSub:
        """Create a subscription queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        # pylint: disable=invalid-name
        q = RabbitMQSub(address, name, auth_token, prefetch)
        await q.connect()
        return q
