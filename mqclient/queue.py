"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import logging
import types
import uuid
from typing import Any, AsyncGenerator, AsyncIterator, Dict, Optional, Type

from . import telemetry as wtt
from .backend_interface import AckException, Backend, Message, NackException, Pub, Sub


class Queue:
    """User-facing queue library.

    Args:
        backend: the backend to use
        address: address of queue
        name: name of queue
        prefetch: size of prefetch buffer for receiving messages
        timeout: seconds to wait for a message to be delivered
        except_errors: whether to suppress interior context errors for
                        the consumer (when `True`, the context manager
                        will act like a `try-except` block)
        auth_token: the (jwt) authentication token
    """

    def __init__(
        self,
        backend: Backend,
        address: str = "localhost",
        name: str = "",
        prefetch: int = 1,
        timeout: int = 60,
        except_errors: bool = True,
        auth_token: str = "",
    ) -> None:
        self._backend = backend
        self._address = address
        self._name = name if name else Queue.make_name()
        self._prefetch = prefetch
        self._auth_token = auth_token

        # publics
        self._timeout = 0
        self.timeout = timeout
        self.except_errors = except_errors

    @staticmethod
    def make_name() -> str:
        """Return a pseudo-unique string that is a legal queue identifier.

        This name is valid for any backend chosen.
        """
        return "a" + (uuid.uuid4().hex)[:20]

    @property
    def timeout(self) -> int:
        """Get the timeout value."""
        return self._timeout

    @timeout.setter
    def timeout(self, val: int) -> None:
        if val < 1:
            raise Exception("prefetch must be positive")
        self._timeout = val

    async def _create_pub_queue(self) -> Pub:
        """Wrap `self._backend.create_pub_queue()` with instance's config."""
        return await self._backend.create_pub_queue(
            self._address, self._name, auth_token=self._auth_token
        )

    async def _create_sub_queue(self) -> Sub:
        """Wrap `self._backend.create_sub_queue()` with instance's config."""
        return await self._backend.create_sub_queue(
            self._address, self._name, self._prefetch, auth_token=self._auth_token
        )

    @contextlib.asynccontextmanager  # needs to wrap @wtt stuff to span children correctly
    @wtt.spanned(
        these=[
            "self._backend",
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
            "self._backend",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
            "msg.msg_id",
        ]
    )  # pylint:disable=no-self-use
    async def _safe_ack(self, sub: Sub, msg: Message) -> None:
        """Acknowledge the message."""
        # pylint:disable=protected-access
        if msg._ack_status == Message.AckStatus.NONE:
            try:
                await sub.ack_message(msg)
                msg._ack_status = Message.AckStatus.ACKED  # mark after success
            except Exception as e:
                raise AckException(f"Acking failed on backend: {msg}") from e
        elif msg._ack_status == Message.AckStatus.NACKED:
            raise AckException(
                f"Message has already been nacked, it cannot be acked: {msg}"
            )
        elif msg._ack_status == Message.AckStatus.ACKED:
            # needless, so we'll skip it
            logging.debug(f"Attempted to ack an already-acked message: {msg}")
        else:
            raise RuntimeError(f"Unrecognized AckStatus value: {msg}")

    @wtt.spanned(
        these=[
            "self._backend",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
            "msg.msg_id",
        ]
    )  # pylint:disable=no-self-use
    async def _safe_nack(self, sub: Sub, msg: Message) -> None:
        """Reject/nack the message."""
        # pylint:disable=protected-access
        if msg._ack_status == Message.AckStatus.NONE:
            try:
                await sub.reject_message(msg)
                msg._ack_status = Message.AckStatus.NACKED  # mark after success
            except Exception as e:
                raise NackException(f"Nacking failed on backend: {msg}") from e
        elif msg._ack_status == Message.AckStatus.NACKED:
            # needless, so we'll skip it
            logging.debug(f"Attempted to nack an already-nacked message: {msg}")
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

        NOTE: If using the GCP backend, a message is allocated for
        redelivery if the consumer's iteration takes longer than 10 minutes.

        Returns:
            QueueSubResource -- context manager and generator object
        """
        logging.debug("Creating new QueueSubResource instance.")
        return QueueSubResource(self)

    @contextlib.asynccontextmanager  # needs to wrap @wtt stuff to span children correctly
    @wtt.spanned(
        these=[
            "self._backend",
            "self._address",
            "self._name",
            "self._prefetch",
            "self.timeout",
        ]
    )
    async def open_sub_one(self) -> AsyncIterator[Message]:
        """Open a context to receive a single messages from the queue.

        This is an async context manager. If an exception is raised
        (inside the context), the message is rejected, the context is
        exited, and exception can be re-raised if configured by
        `except_errors`.

        NOTE: If using the GCP backend, a message is allocated for
        redelivery if the context is open for longer than 10 minutes.

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
        else:  # got a message -> link and proceed
            msg = add_span_link(raw_msg)

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

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return (
            f"Queue("
            f"{self._backend.__module__}, "
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
        msg = Message.serialize(data, headers=wtt.inject_links_carrier())
        await self.pub.send_message(msg)


class QueueSubResource:
    """An async context manager wrapping `Sub.message_generator()`."""

    RUNTIME_ERROR_CONTEXT_STRING = (
        "'QueueSubResource' object's runtime "
        "context has not been entered. Use 'async with ... as ...' syntax."
    )

    def __init__(self, queue: Queue) -> None:
        logging.debug("[QueueSubResource.__init__()]")
        self.queue = queue

        self._sub: Optional[Sub] = None
        self._gen: Optional[AsyncGenerator[Optional[Message], None]] = None

        self._span: Optional[wtt.Span] = None
        self._span_carrier: Optional[Dict[str, Any]] = None

        self.msg: Optional[Message] = None

    @wtt.spanned(
        these=[
            "self.queue._backend",
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
        logging.debug("[QueueSubResource.__aenter__()] entered `with-as` block")

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
        logging.debug(
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
            logging.debug("[QueueSubResource.__aexit__()] exited & propagated error.")
            return False  # propagate the Exception!
        else:
            # either no exception or suppress the exception
            if exc_type and exc_val:
                logging.debug(
                    "[QueueSubResource.__aexit__()] exited & suppressed error."
                )
            else:
                logging.debug("[QueueSubResource.__aexit__()] exited w/o error.")
            return True  # suppress any Exception

    def __aiter__(self) -> "QueueSubResource":
        """Return instance.

        Triggered with 'for'/'aiter()'.
        """
        logging.debug("[QueueSubResource.__aiter__()] entered loop/`aiter()`")
        if not (self._sub and self._gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)
        return self

    @wtt.spanned(
        these=[
            "self.queue._backend",
            "self.queue._address",
            "self.queue._name",
            "self.queue._prefetch",
            "self.queue.timeout",
        ],
        carrier="self._span_carrier",
    )
    async def __anext__(self) -> Any:
        """Return next Message in queue."""
        logging.debug("[QueueSubResource.__anext__()] next iteration...")
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
        def get_message_callback(msg: Message) -> Message:
            return msg

        try:
            self.msg = get_message_callback(await self._gen.__anext__())
        except StopAsyncIteration:
            self.msg = None  # signal there is no message to ack/nack in `__aexit__()`
            logging.debug(
                "[QueueSubResource.__anext__()] end of loop (StopAsyncIteration)"
            )
            raise

        if not self.msg:
            raise RuntimeError(
                "Yielded value is `None`. This should not have happened."
            )

        return self.msg.data

    @wtt.spanned(
        these=[
            "self.queue._backend",
            "self.queue._address",
            "self.queue._name",
            "self.queue._prefetch",
            "self.queue.timeout",
        ],
    )
    async def nack_current(self) -> None:
        """Manually nack the current (most recently yielded) message."""
        # pylint:disable=protected-access
        await self.queue._safe_nack(self._sub, self.msg)
