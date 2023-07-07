"""Back-end using RabbitMQ."""

import functools
import logging
import urllib
from typing import Any, AsyncGenerator, AsyncIterator, Dict, Optional, Tuple, Union

import pika  # type: ignore

from .. import broker_client_interface, log_msgs
from ..broker_client_interface import (
    RETRIES,
    RETRY_DELAY,
    TIMEOUT_MILLIS_DEFAULT,
    AlreadyClosedException,
    ClosingFailedException,
    ConnectingFailedException,
    Message,
    Pub,
    RawQueue,
    Sub,
)
from . import utils

StrDict = Dict[str, Any]

LOGGER = logging.getLogger("mqclient.rabbitmq")


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
        raise RuntimeError(f"Invalid address: {url} (format: {HUMAN_PATTERN})")

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
        raise RuntimeError("username given but no password or token")
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
        ack_timeout: Optional[int],
    ) -> None:
        super().__init__()
        LOGGER.info(f"Requested MQClient for queue '{queue}' @ {address}")
        cp_args, username, password = _parse_url(address)

        # set up connection parameters
        if creds := _get_credentials(username, password, auth_token):
            cp_args["credentials"] = creds
        if ack_timeout:
            cp_args["heartbeat"] = ack_timeout
        self.parameters = pika.connection.ConnectionParameters(**cp_args)

        self.queue = queue
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    async def connect(self) -> None:
        """Set up connection and channel."""
        await super().connect()
        LOGGER.info(f"Connecting with parameters={self.parameters}")
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        """
        We need to discuss how many RabbitMQ instances we want to run
        the default is that the quorum queue is spread across 3 nodes
        so 1 can fail without issue. Maybe we want to up this for
        more production workloads
        """
        self.channel.queue_declare(
            queue=self.queue, durable=True, arguments={"x-queue-type": "quorum"}
        )

    async def close(self) -> None:
        """Close connection."""
        await super().close()

        if not self.channel:
            raise ClosingFailedException("No channel to close.")
        if not self.connection:
            raise ClosingFailedException("No connection to close.")
        if self.connection.is_closed:
            raise AlreadyClosedException()

        try:
            self.channel.cancel()
            self.connection.close()
        except Exception as e:
            raise ClosingFailedException() from e

        if self.channel.is_open:
            LOGGER.warning("Channel remains open after connection close.")


class RabbitMQPub(RabbitMQ, Pub):
    """Wrapper around queue with delivery-confirm mode in the channel.

    Extends:
        RabbitMQ
        Pub
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        LOGGER.debug(f"{log_msgs.INIT_PUB} ({args}; {kwargs})")
        super().__init__(*args, **kwargs)

    async def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on delivery confirmations.
        """
        LOGGER.debug(log_msgs.CONNECTING_PUB)
        await super().connect()

        if not self.channel:
            raise ConnectingFailedException("No channel to configure connection.")

        self.channel.confirm_delivery()

        LOGGER.debug(log_msgs.CONNECTED_PUB)

    async def close(self) -> None:
        """Close connection."""
        LOGGER.debug(log_msgs.CLOSING_PUB)
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_PUB)

    async def send_message(
        self,
        msg: bytes,
        retries: int = RETRIES,
        retry_delay: int = RETRY_DELAY,
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
            raise RuntimeError("queue is not connected")

        await utils.auto_retry_call(
            func=functools.partial(
                self.channel.basic_publish,
                exchange="",
                routing_key=self.queue,
                body=msg,
            ),
            nonretriable_conditions=lambda e: isinstance(
                e, pika.exceptions.AMQPChannelError
            ),
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            logger=LOGGER,
        )
        LOGGER.debug(log_msgs.SENT_MESSAGE)


class RabbitMQSub(RabbitMQ, Sub):
    """Wrapper around queue with prefetch-queue QoS.

    Extends:
        RabbitMQ
        Sub
    """

    def __init__(self, *args: Any, prefetch: int, **kwargs: Any) -> None:
        LOGGER.debug(f"{log_msgs.INIT_SUB} ({args}; {kwargs})")
        super().__init__(*args, **kwargs)
        self.consumer_id = None
        self._prefetch = prefetch  # see `Sub.prefetch` property

    async def connect(self) -> None:
        """Set up connection, channel, and queue.

        Turn on prefetching.
        """
        LOGGER.debug(log_msgs.CONNECTING_SUB)
        await super().connect()

        if not self.channel:
            raise ConnectingFailedException("No channel to configure connection.")

        self.channel.basic_qos(prefetch_count=self.prefetch)
        # global_qos=False b/c using quorum queues
        # https://www.rabbitmq.com/quorum-queues.html#global-qos

        LOGGER.debug(log_msgs.CONNECTED_SUB)

    async def close(self) -> None:
        """Close connection.

        Also, channel will be canceled (rejects all pending ackable
        messages).
        """
        LOGGER.debug(log_msgs.CLOSING_SUB)
        await super().close()
        LOGGER.debug(log_msgs.CLOSED_SUB)

    @staticmethod
    def _to_message(  # type: ignore[override]  # noqa: F821 # pylint: disable=W0221
        method_frame: Optional[pika.spec.Basic.GetOk], body: Optional[Union[str, bytes]]
    ) -> Optional[Message]:
        """Transform RabbitMQ-Message to Message type."""
        if not method_frame or body is None:
            return None

        if isinstance(body, str):
            return Message(method_frame.delivery_tag, body.encode())
        else:
            return Message(method_frame.delivery_tag, body)

    async def _iter_messages(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: int,
    ) -> AsyncIterator[Optional[Message]]:
        if not self.channel:
            raise RuntimeError("queue is not connected")

        iterator = self.channel.consume(
            self.queue,
            inactivity_timeout=timeout_millis / 1000.0 if timeout_millis else None,
        )

        while True:
            try:
                pika_msg = await utils.auto_retry_call(
                    func=functools.partial(next, iterator),
                    nonretriable_conditions=lambda e: isinstance(
                        e, (pika.exceptions.AMQPChannelError, StopIteration)
                    ),
                    retries=retries,
                    retry_delay=retry_delay,
                    close=self.close,
                    connect=self.connect,
                    logger=LOGGER,
                )
            except StopIteration:
                return
            if msg := RabbitMQSub._to_message(pika_msg[0], pika_msg[2]):  # type: ignore[index]
                LOGGER.debug(f"{log_msgs.GETMSG_RECEIVED_MESSAGE} ({msg.msg_id!r}).")
                yield msg
            else:
                LOGGER.debug(log_msgs.GETMSG_NO_MESSAGE)
                yield None

    async def get_message(
        self,
        timeout_millis: Optional[int] = TIMEOUT_MILLIS_DEFAULT,
        retries: int = RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> Optional[Message]:
        """Get a message from a queue."""
        LOGGER.debug(log_msgs.GETMSG_RECEIVE_MESSAGE)
        if not self.channel:
            raise RuntimeError("queue is not connected")

        msg = None
        async for msg in self._iter_messages(timeout_millis, retries, retry_delay):
            # None -> timeout
            break  # get just one message

        # self.channel.cancel()  # this is done by `open_sub_one()` *after* ack/nack via `close()`

        return msg

    async def ack_message(
        self,
        msg: Message,
        retries: int = RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> None:
        """Ack a message from the queue.

        Note that RabbitMQ acks messages in-order, so acking message 3
        of 3 in-progress messages will ack them all.
        """
        LOGGER.debug(log_msgs.ACKING_MESSAGE)
        if not self.channel:
            raise RuntimeError("queue is not connected")

        await utils.auto_retry_call(
            func=functools.partial(
                self.channel.basic_ack,
                msg.msg_id,
            ),
            nonretriable_conditions=lambda e: isinstance(
                e, pika.exceptions.AMQPChannelError
            ),
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.ACKED_MESSAGE} ({msg.msg_id!r}).")

    async def reject_message(
        self,
        msg: Message,
        retries: int = RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> None:
        """Reject (nack) a message from the queue.

        Note that RabbitMQ acks messages in-order, so nacking message 3
        of 3 in-progress messages will nack them all.
        """
        LOGGER.debug(log_msgs.NACKING_MESSAGE)
        if not self.channel:
            raise RuntimeError("queue is not connected")

        await utils.auto_retry_call(
            func=functools.partial(
                self.channel.basic_nack,
                msg.msg_id,
            ),
            nonretriable_conditions=lambda e: isinstance(
                e, pika.exceptions.AMQPChannelError
            ),
            retries=retries,
            retry_delay=retry_delay,
            close=self.close,
            connect=self.connect,
            logger=LOGGER,
        )
        LOGGER.debug(f"{log_msgs.NACKED_MESSAGE} ({msg.msg_id!r}).")

    async def message_generator(
        self,
        timeout: int = 60,
        propagate_error: bool = True,
        retries: int = RETRIES,
        retry_delay: int = RETRY_DELAY,
    ) -> AsyncGenerator[Optional[Message], None]:
        """Yield Messages.

        Generate messages with variable timeout.
        Yield `None` on `throw()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            propagate_error -- should errors from downstream kill the generator? (default: {True})
        """
        LOGGER.debug(log_msgs.MSGGEN_ENTERED)
        if not self.channel:
            raise RuntimeError("queue is not connected")

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
            self.channel.cancel()


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
        ack_timeout: Optional[int],
    ) -> RabbitMQPub:
        """Create a publishing queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        # pylint: disable=invalid-name
        q = RabbitMQPub(address, name, auth_token, ack_timeout)
        await q.connect()
        return q

    @staticmethod
    async def create_sub_queue(
        address: str,
        name: str,
        prefetch: int,
        auth_token: str,
        ack_timeout: Optional[int],
    ) -> RabbitMQSub:
        """Create a subscription queue.

        Args:
            address (str): address of queue
            name (str): name of queue on address

        Returns:
            RawQueue: queue
        """
        # pylint: disable=invalid-name
        q = RabbitMQSub(address, name, auth_token, ack_timeout, prefetch=prefetch)
        await q.connect()
        return q
