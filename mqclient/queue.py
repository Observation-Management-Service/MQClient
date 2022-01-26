"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import logging
import types
import uuid
from typing import Any, AsyncGenerator, AsyncIterator, Dict, Optional, Type

import wipac_telemetry.tracing_tools as wtt

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
    async def sender(self) -> AsyncIterator["QueueSender"]:
        """Send messages to the queue."""
        pub = await self._create_pub_queue()

        try:
            yield QueueSender(pub)
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
    async def ack(self, sub: Sub, msg: Message) -> None:
        """Acknowledge the message."""
        if msg.ack_status == Message.AckStatus.NONE:
            try:
                await sub.ack_message(msg)
            except Exception as e:
                raise AckException("Acking failed on backend") from e
        elif msg.ack_status == Message.AckStatus.NACKED:
            raise AckException(
                "Message has already been rejected/nacked, it cannot be acked"
            )
        elif msg.ack_status == Message.AckStatus.ACKED:
            pass  # needless, so we'll skip it
        else:
            raise RuntimeError(f"Unrecognized AckStatus value: {msg.ack_status}")

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
    async def nack(self, sub: Sub, msg: Message) -> None:
        """Reject/nack the message."""
        if msg.ack_status == Message.AckStatus.NONE:
            try:
                await sub.reject_message(msg)
            except Exception as e:
                raise NackException("Nacking failed on backend") from e
        elif msg.ack_status == Message.AckStatus.NACKED:
            pass  # needless, so we'll skip it
        elif msg.ack_status == Message.AckStatus.ACKED:
            raise NackException(
                "Message has already been acked, it cannot be rejected/nacked"
            )
        else:
            raise RuntimeError(f"Unrecognized AckStatus value: {msg.ack_status}")

    def recv(self) -> "MessageAsyncGeneratorContext":
        """Receive a stream of messages from the queue.

        This returns a context-manager/generator. Its iterator stops when no
        messages are received for `timeout` seconds. If an exception is raised
        (inside the context), the message is rejected, the context is exited,
        and exception can be re-raised if configured by `except_errors`.
        Multiple calls to `recv()` is okay, but reusing the returned instance
        is not.

        Example:
            async with queue.recv() as stream:
                async for data in stream:
                    ...

        NOTE: If using the GCP backend, a message is allocated for
        redelivery if the consumer's iteration takes longer than 10 minutes.

        Returns:
            MessageAsyncGeneratorContext -- context manager and generator object
        """
        logging.debug("Creating new MessageAsyncGeneratorContext instance.")
        return MessageAsyncGeneratorContext(self)

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
    async def recv_one(self) -> AsyncIterator[Message]:
        """Receive one message from the queue.

        This is an async context manager. If an exception is raised
        (inside the context), the message is rejected, the context is
        exited, and exception can be re-raised if configured by
        `except_errors`.

        NOTE: If using the GCP backend, a message is allocated for
        redelivery if the context is open for longer than 10 minutes.

        Decorators:
            contextlib.asynccontextmanager

        Yields:
            Any -- object of data received, or None if queue is empty
        """

        @wtt.spanned(
            kind=wtt.SpanKind.CONSUMER,
            carrier="msg.headers",
            carrier_relation=wtt.CarrierRelation.LINK,
        )
        def get_message_callback(msg: Message) -> Message:
            if not msg:
                raise Exception("No message available")
            return msg

        sub = await self._create_sub_queue()
        msg = get_message_callback(await sub.get_message(self.timeout * 1000))

        data = msg.data
        try:
            yield data
        except Exception:  # pylint:disable=broad-except
            await self.nack(sub, msg)
            if not self.except_errors:
                raise
        else:
            await self.ack(sub, msg)
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


class QueueSender:
    def __init__(self, pub: Pub):
        self.pub = pub

    @wtt.spanned(kind=wtt.SpanKind.PRODUCER)
    async def send(self, data: Any) -> None:
        """Send a message."""
        msg = Message.serialize(data, headers=wtt.inject_links_carrier())
        await self.pub.send_message(msg)


class MessageAsyncGeneratorContext:
    """An async context manager wrapping `Sub.message_generator()`."""

    RUNTIME_ERROR_CONTEXT_STRING = (
        "'MessageAsyncGeneratorContext' object's runtime "
        "context has not been entered. Use 'async with ... as ...' syntax."
    )

    def __init__(self, queue: Queue) -> None:
        logging.debug("[MessageAsyncGeneratorContext.__init__()]")
        self.queue = queue

        self.sub: Optional[Sub] = None
        self.gen: Optional[AsyncGenerator[Optional[Message], None]] = None

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
    async def __aenter__(self) -> "MessageAsyncGeneratorContext":
        """Return instance.

        Triggered by 'with ... as'.
        """
        logging.debug(
            "[MessageAsyncGeneratorContext.__aenter__()] entered `with-as` block"
        )

        if self.sub and self.gen:
            raise RuntimeError(
                "A 'MessageAsyncGeneratorContext' instance cannot be re-entered."
            )

        self.sub = await self.queue._create_sub_queue()
        self.gen = self.sub.message_generator(
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
            f"[MessageAsyncGeneratorContext.__aexit__()] exiting `with-as` block (exc:{exc_type})"
        )
        if not (self.sub and self.gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        reraise_exception = False

        # Exception Was Raised
        if exc_type and exc_val:
            if self.msg:
                await self.queue.nack(self.sub, self.msg)
            # see how the generator wants to handle the exception
            try:
                # `athrow` is caught by the generator's try-except around `yield`
                await self.gen.athrow(exc_type, exc_val, exc_tb)
            except exc_type:  # message_generator re-raised Exception
                reraise_exception = True
        # Good Exit (No Original Exception)
        else:
            if self.msg:
                await self.queue.ack(self.sub, self.msg)

        await self.sub.close()  # close after cleanup

        if reraise_exception:
            logging.debug(
                "[MessageAsyncGeneratorContext.__aexit__()] exited & propagated error."
            )
            return False  # propagate the Exception!
        else:
            # either no exception or suppress the exception
            if exc_type and exc_val:
                logging.debug(
                    "[MessageAsyncGeneratorContext.__aexit__()] exited & suppressed error."
                )
            else:
                logging.debug(
                    "[MessageAsyncGeneratorContext.__aexit__()] exited w/o error."
                )
            return True  # suppress any Exception

    def __aiter__(self) -> "MessageAsyncGeneratorContext":
        """Return instance.

        Triggered with 'for'/'aiter()'.
        """
        logging.debug(
            "[MessageAsyncGeneratorContext.__aiter__()] entered loop/`aiter()`"
        )
        if not (self.sub and self.gen):
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
        logging.debug("[MessageAsyncGeneratorContext.__anext__()] next iteration...")
        if not (self.sub and self.gen):
            raise RuntimeError(self.RUNTIME_ERROR_CONTEXT_STRING)

        # ack the previous message before getting a new one
        if self.msg:
            await self.queue.ack(self.sub, self.msg)

        @wtt.spanned(
            kind=wtt.SpanKind.CONSUMER,
            carrier="msg.headers",
            carrier_relation=wtt.CarrierRelation.LINK,
        )
        def get_message_callback(msg: Message) -> Message:
            return msg

        try:
            self.msg = get_message_callback(await self.gen.__anext__())
        except StopAsyncIteration:
            self.msg = None  # signal there is no message to ack/nack in `__aexit__()`
            logging.debug(
                "[MessageAsyncGeneratorContext.__anext__()] end of loop (StopAsyncIteration)"
            )
            raise

        if not self.msg:
            raise RuntimeError(
                "Yielded value is `None`. This should not have happened."
            )

        return self.msg.data
