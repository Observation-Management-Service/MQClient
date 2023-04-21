"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import logging
import sys
import types
import uuid
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Type,
)

from . import broker_client_manager
from . import telemetry as wtt
from .broker_client_interface import AckException, Message, NackException, Pub, Sub

LOGGER = logging.getLogger("mqclient")


def _message_size_message(msg: Message) -> str:
    return (
        f"{sys.getsizeof(msg.payload)} bytes "
        f"(data={sys.getsizeof(msg.data)}, headers={sys.getsizeof(msg.headers)}) "
        f"[msg_id={msg.msg_id!r}]"
    )


class Queue:
    """User-facing queue library.

    Args:
        broker_client: the broker_client to use
        address: address of queue
        name: name of queue
        prefetch: size of prefetch buffer for receiving messages
        timeout: seconds to wait for a message to be delivered
        ack_timeout: max time (seconds) to acknowledge a message
                     before broker considers it lost (and re-queues)
        except_errors: whether to suppress interior context errors for
                        the consumer (when `True`, the context manager
                        will act like a `try-except` block)
        auth_token: the (jwt) authentication token
    """

    def __init__(
        self,
        broker_client: str,
        address: str = "localhost",
        name: str = "",
        prefetch: int = 1,
        timeout: int = 60,
        ack_timeout: Optional[int] = None,
        except_errors: bool = True,
        auth_token: str = "",
    ) -> None:
        self._broker_client = broker_client_manager.get_broker_client(broker_client)
        self._address = address
        self._name = name if name else Queue.make_name()
        self._prefetch = prefetch
        self._auth_token = auth_token

        if ack_timeout is not None and ack_timeout <= 0:
            raise ValueError("timeout must be positive")
        self._ack_timeout = ack_timeout

        # publics
        self._timeout = 0
        self.timeout = timeout
        self.except_errors = except_errors

    @staticmethod
    def make_name() -> str:
        """Return a pseudo-unique string that is a legal queue identifier.

        This name is valid for any broker_client chosen.
        """
        return "a" + (uuid.uuid4().hex)[:20]

    @property
    def timeout(self) -> int:
        """Get the timeout value."""
        return self._timeout

    @timeout.setter
    def timeout(self, val: int) -> None:
        LOGGER.debug(f"Setting timeout to {val}")
        if val < 1:
            raise ValueError("timeout must be positive")
        self._timeout = val

    async def _create_pub_queue(self) -> Pub:
        """Wrap `self._broker_client.create_pub_queue()` with instance's
        config."""
        return await self._broker_client.create_pub_queue(
            self._address,
            self._name,
            self._auth_token,
            self._ack_timeout,
        )

    async def _create_sub_queue(self) -> Sub:
        """Wrap `self._broker_client.create_sub_queue()` with instance's
        config."""
        return await self._broker_client.create_sub_queue(
            self._address,
            self._name,
            self._prefetch,
            self._auth_token,
            self._ack_timeout,
        )

    @contextlib.asynccontextmanager  # needs to wrap @wtt stuff to span children correctly
    @wtt.spanned(
        these=[
            "self._broker_client",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
        ]
    )
    async def open_pub(self) -> AsyncIterator["QueuePubResource"]:
        """Open a resource to send messages to the queue.

        This is an async context manager. An object is returned that can be
        used to send n messages.

        Example:
            async with queue.open_pub() as p:
                for msg in my_messages:
                    await p.send(msg)

        Decorators:
            contextlib.asynccontextmanager

        Returns:
            QueuePubResource -- the object to invoke `.send()` on
        """
        pub = await self._create_pub_queue()

        try:
            yield QueuePubResource(pub)
        finally:
            await pub.close()

    @wtt.spanned(
        these=[
            "self._broker_client",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
            "msg.msg_id",
        ]
    )
    async def _safe_ack(self, sub: Sub, msg: Message) -> None:
        """Acknowledge the message."""
        # pylint:disable=protected-access
        if msg._ack_status == Message.AckStatus.NONE:
            try:
                await sub.ack_message(msg)
                msg._ack_status = Message.AckStatus.ACKED  # mark after success
            except Exception as e:
                raise AckException(f"Acking failed on broker_client: {msg}") from e
        elif msg._ack_status == Message.AckStatus.NACKED:
            raise AckException(
                f"Message has already been nacked, it cannot be acked: {msg}"
            )
        elif msg._ack_status == Message.AckStatus.ACKED:
            # needless, so we'll skip it
            LOGGER.debug(f"Attempted to ack an already-acked message: {msg}")
        else:
            raise RuntimeError(f"Unrecognized AckStatus value: {msg}")

    @wtt.spanned(
        these=[
            "self._broker_client",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
            "msg.msg_id",
        ]
    )
    async def _safe_nack(self, sub: Sub, msg: Message) -> None:
        """Reject/nack the message."""
        # pylint:disable=protected-access
        if msg._ack_status == Message.AckStatus.NONE:
            try:
                await sub.reject_message(msg)
                msg._ack_status = Message.AckStatus.NACKED  # mark after success
            except Exception as e:
                raise NackException(f"Nacking failed on broker_client: {msg}") from e
        elif msg._ack_status == Message.AckStatus.NACKED:
            # needless, so we'll skip it
            LOGGER.debug(f"Attempted to nack an already-nacked message: {msg}")
        elif msg._ack_status == Message.AckStatus.ACKED:
            raise NackException(
                f"Message has already been acked, it cannot be nacked: {msg}"
            )
        else:
            raise RuntimeError(f"Unrecognized AckStatus value: {msg}")

    def open_sub(self) -> "QueueSubResource":
        """Open a resource to receive messages from the queue as an iterator.

        This returns a context-manager/generator. Its iterator stops when no
        messages are received for `timeout` seconds. If an exception is raised
        (inside the context), the message is rejected, the context is exited,
        and exception can be re-raised if configured by `except_errors`.
        Multiple calls to `open_sub()` is okay, but reusing the returned
        instance is not.

        Example:
            async with queue.open_sub() as stream:
                async for msg in stream:
                    print(msg)

        Returns:
            QueueSubResource -- context manager and generator object
        """
        LOGGER.debug("Creating new QueueSubResource instance.")
        return QueueSubResource(self)

    @contextlib.asynccontextmanager  # needs to wrap @wtt stuff to span children correctly
    @wtt.spanned(
        these=[
            "self._broker_client",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
        ]
    )
    async def open_sub_one(self) -> AsyncIterator[Any]:
        """Open a context to receive a single messages from the queue.

        This is an async context manager. If an exception is raised
        (inside the context), the message is rejected, the context is
        exited, and exception can be re-raised if configured by
        `except_errors`.

        Example:
            async with q.open_sub_one() as msg:
                print(msg)

        Decorators:
            contextlib.asynccontextmanager

        Raises:
            EmptyQueueException -- if there is no available message

        Yields:
            Any -- object of data received
        """

        @wtt.spanned(
            kind=wtt.SpanKind.CONSUMER,
            carrier="msg.headers",
            carrier_relation=wtt.CarrierRelation.LINK,
        )
        def add_span_link(msg: Message) -> Message:
            return msg

        sub = await self._create_sub_queue()
        raw_msg = await sub.get_message(self.timeout * 1000)

        if not raw_msg:  # no message -> close and exit
            await sub.close()
            raise EmptyQueueException(
                "No message is available (`timeout` value may be too low)"
            )

        msg = add_span_link(raw_msg)  # got a message -> link and proceed
        LOGGER.info(f"Received Message: {_message_size_message(msg)}")

        try:
            yield msg.data
        except Exception:  # pylint:disable=broad-except
            await self._safe_nack(sub, msg)
            if not self.except_errors:
                raise
        else:
            await self._safe_ack(sub, msg)
        finally:
            await sub.close()

    @contextlib.asynccontextmanager  # needs to wrap @wtt stuff to span children correctly
    @wtt.spanned(
        these=[
            "self._broker_client",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
        ]
    )
    async def open_sub_manual_acking(self) -> AsyncIterator["ManualQueueSubResource"]:
        """A bare-bones sub function.

        TODO - elaborate

        All acking and/or nacking needs to be done by caller.
        """
        sub = await self._create_sub_queue()

        try:
            yield ManualQueueSubResource(
                lambda: sub.get_message(self.timeout * 1000),
                lambda msg: self._safe_ack(sub, msg),
                lambda msg: self._safe_nack(sub, msg),
            )
        finally:
            await sub.close()

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return (
            f"Queue("
            f"{self._broker_client.__module__}, "
            f"address={self._address}, "
            f"name={self._name}, "
            f"prefetch={self._prefetch}, "
            f"timeout={self.timeout}"
            f")"
        )


class EmptyQueueException(Exception):
    """Raised when the queue is empty."""


class QueuePubResource:
    """A manager class around `Pub.send_message()`."""

    def __init__(self, pub: Pub):
        self.pub = pub

    @wtt.spanned(kind=wtt.SpanKind.PRODUCER)
    async def send(self, data: Any) -> None:
        """Send a message."""
        msg_bytes = Message.serialize(data, headers=wtt.inject_links_carrier())
        LOGGER.info(f"Sending Message: {sys.getsizeof(msg_bytes)} bytes")
        await self.pub.send_message(msg_bytes)


class ManualQueueSubResource:
    """A manager class around `Sub.get_message()`."""

    def __init__(
        self,
        get_message_function: Callable[[], Awaitable[Optional[Message]]],
        ack_function: Callable[[Message], Awaitable[None]],
        nack_function: Callable[[Message], Awaitable[None]],
    ) -> None:
        self._get_message = get_message_function
        self.ack = ack_function
        self.nack = nack_function

    async def next(self) -> AsyncIterator[Message]:
        """Yield a message."""

        @wtt.spanned(
            kind=wtt.SpanKind.CONSUMER,
            carrier="msg.headers",
            carrier_relation=wtt.CarrierRelation.LINK,
        )
        def add_span_link(msg: Message) -> Message:
            return msg

        while True:
            raw_msg = await self._get_message()
            if not raw_msg:  # no message -> close and exit
                return

            msg = add_span_link(raw_msg)  # got a message -> link and proceed
            LOGGER.info(f"Received Message: {_message_size_message(msg)}")
            yield msg


class QueueSubResource:
    """An async context-manager generator, wraps `Sub.message_generator()`."""

    RUNTIME_ERROR_CONTEXT_STRING = (
        "'QueueSubResource' object's runtime "
        "context has not been entered. Use 'async with ... as ...' syntax."
    )

    def __init__(self, queue: Queue) -> None:
        LOGGER.debug("[QueueSubResource.__init__()]")
        self.queue = queue

        self._sub: Optional[Sub] = None
        self._gen: Optional[AsyncGenerator[Optional[Message], None]] = None

        self._span: Optional[wtt.Span] = None
        self._span_carrier: Optional[Dict[str, Any]] = None

        self.msg: Optional[Message] = None

    @wtt.spanned(
        these=[
            "self.queue._broker_client",
            "self.queue._address",
            "self.queue._name",
            "self.queue._prefetch",
            "self.queue.timeout",
        ],
        behavior=wtt.SpanBehavior.ONLY_END_ON_EXCEPTION,
    )
    async def __aenter__(self) -> "QueueSubResource":
        """Return instance.

        Triggered by 'with ... as'.
        """
        LOGGER.debug("[QueueSubResource.__aenter__()] entered `with-as` block")

        if self._sub and self._gen:
            raise RuntimeError("A 'QueueSubResource' instance cannot be re-entered.")

        self._sub = await self.queue._create_sub_queue()
        self._gen = self._sub.message_generator(
            timeout=self.queue.timeout,
            propagate_error=(not self.queue.except_errors),
        )

        self._span = wtt.get_current_span()
        self._span_carrier = wtt.inject_span_carrier()

        return self

    @wtt.respanned(
        "self._span",
        behavior=wtt.SpanBehavior.END_ON_EXIT,  # end what was opened by `__aenter__()`
    )
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> bool:
        """Return `True` to suppress any Exception raised by consumer code.

        Return `False` to re-raise/propagate that Exception.

        Arguments:
            exc_type {Optional[BaseException]} -- Exception type.
            exc_val {Optional[Type[BaseException]]} -- Exception object.
            exc_tb {Optional[types.TracebackType]} -- Exception Traceback.
        """
        LOGGER.debug(
            f"[QueueSubResource.__aexit__()] exiting `with-as` block (exc:{exc_type})"
        )
        if not (self._sub and self._gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        reraise_exception = False

        # Exception Was Raised
        if exc_type and exc_val:
            if self.msg:
                await self.queue._safe_nack(self._sub, self.msg)
            # see how the generator wants to handle the exception
            try:
                # `athrow` is caught by the generator's try-except around `yield`
                await self._gen.athrow(exc_type, exc_val, exc_tb)
            except exc_type:  # message_generator re-raised Exception
                reraise_exception = True
        # Good Exit (No Original Exception)
        else:
            # ack if there was a message yielded (unless it was already nacked)
            if self.msg and self.msg._ack_status != Message.AckStatus.NACKED:
                await self.queue._safe_ack(self._sub, self.msg)

        await self._sub.close()  # close after cleanup

        if reraise_exception:
            LOGGER.debug("[QueueSubResource.__aexit__()] exited & propagated error.")
            return False  # propagate the Exception!
        else:
            # either no exception or suppress the exception
            if exc_type and exc_val:
                LOGGER.debug(
                    "[QueueSubResource.__aexit__()] exited & suppressed error."
                )
            else:
                LOGGER.debug("[QueueSubResource.__aexit__()] exited w/o error.")
            return True  # suppress any Exception

    def __aiter__(self) -> "QueueSubResource":
        """Return instance.

        Triggered with 'for'/'aiter()'.
        """
        LOGGER.debug("[QueueSubResource.__aiter__()] entered loop/`aiter()`")
        if not (self._sub and self._gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)
        return self

    @wtt.spanned(
        these=[
            "self.queue._broker_client",
            "self.queue._address",
            "self.queue._name",
            "self.queue._prefetch",
            "self.queue.timeout",
        ],
        carrier="self._span_carrier",
    )
    async def __anext__(self) -> Any:
        """Return next Message in queue."""
        LOGGER.debug("[QueueSubResource.__anext__()] next iteration...")
        if not (self._sub and self._gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        # ack the previous message before getting a new one (unless it was already nacked)
        if self.msg and self.msg._ack_status != Message.AckStatus.NACKED:
            await self.queue._safe_ack(self._sub, self.msg)

        @wtt.spanned(
            kind=wtt.SpanKind.CONSUMER,
            carrier="msg.headers",
            carrier_relation=wtt.CarrierRelation.LINK,
        )
        def get_message_callback(msg: Optional[Message]) -> Optional[Message]:
            return msg

        try:
            self.msg = get_message_callback(await self._gen.__anext__())
        except StopAsyncIteration:
            self.msg = None  # signal there is no message to ack/nack in `__aexit__()`
            LOGGER.debug(
                "[QueueSubResource.__anext__()] end of loop (StopAsyncIteration)"
            )
            raise

        if not self.msg:
            raise RuntimeError(
                "Yielded value is `None`. This should not have happened."
            )

        LOGGER.info(f"Received Message: {_message_size_message(self.msg)}")
        return self.msg.data

    @wtt.spanned(
        these=[
            "self.queue._broker_client",
            "self.queue._address",
            "self.queue._name",
            "self.queue._prefetch",
            "self.queue.timeout",
        ],
    )
    async def nack_current(self) -> None:
        """Manually nack the current (most recently yielded) message."""
        if not (self._sub and self._gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)
        if not self.msg:  # case: calling after iterator stopped (unusual but possible)
            return
        # pylint:disable=protected-access
        await self.queue._safe_nack(self._sub, self.msg)
