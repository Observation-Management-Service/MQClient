"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import logging
import pickle
import uuid
from typing import Any, Generator, Optional

from .backend_interface import Backend, MessageGeneratorContext, Pub, Sub


class Queue:
    """User-facing queue library.

    Args:
        backend (Backend): the backend to use
        address (str): address of queue (default: 'localhost')
        name (str): name of queue (default: <random string>)
        prefetch (int): size of prefetch buffer for receiving messages (default: 1)
    """

    def __init__(self, backend: Backend, address: str = 'localhost',
                 name: str = '', prefetch: int = 1) -> None:
        self._backend = backend
        self._address = address
        self._name = name if name else uuid.uuid4().hex
        self._prefetch = prefetch
        self._pub_queue = None  # type: Optional[Pub]
        self._sub_queue = None  # type: Optional[Sub]
        self.message_generator_context = None  # type: Optional[MessageGeneratorContext]
        self._propagate_recv_error = False

    @property
    def backend(self) -> Backend:
        """Get backend instance responsible for managing queuing service."""
        return self._backend

    @property
    def address(self) -> str:
        """Get address of the queuing daemon."""
        return self._address

    @property
    def name(self) -> str:
        """Get name of queue."""
        return self._name

    @property
    def prefetch(self) -> int:
        """Get size of prefetch buffer for receiving messages."""
        return self._prefetch

    @prefetch.setter
    def prefetch(self, val: int) -> None:
        if val < 1:
            raise Exception('prefetch must be positive')
        if self._prefetch != val:
            self._prefetch = val
            if self._sub_queue:
                del self.raw_sub_queue

    @property
    def raw_pub_queue(self) -> Pub:
        """Get publisher queue."""
        if not self._pub_queue:
            self._pub_queue = self._backend.create_pub_queue(self._address, self._name)

        if not self._pub_queue:
            raise Exception("Pub queue failed to be created.")
        return self._pub_queue

    @raw_pub_queue.deleter
    def raw_pub_queue(self) -> None:
        logging.debug("Deleter Queue.raw_pub_queue")
        self._close_pub_queue()

    def _close_pub_queue(self) -> None:
        if self._pub_queue:
            logging.debug("Closing Queue._pub_queue")
            self._pub_queue.close()
            self._pub_queue = None

    @property
    def raw_sub_queue(self) -> Sub:
        """Get subscriber queue."""
        if not self._sub_queue:
            self._sub_queue = self._backend.create_sub_queue(
                self._address, self._name, self._prefetch)

        if not self._sub_queue:
            raise Exception("Sub queue failed to be created.")
        return self._sub_queue

    @raw_sub_queue.deleter
    def raw_sub_queue(self) -> None:
        logging.debug("Deleter Queue.raw_sub_queue")
        self._close_sub_queue()

    def _close_sub_queue(self) -> None:
        if self._sub_queue:
            logging.debug("Closing Queue._sub_queue")
            self._sub_queue.close()
            self._sub_queue = None

    def close(self) -> None:
        """Close all connections."""
        self._close_sub_queue()
        self._close_pub_queue()

    def send(self, data: Any) -> None:
        """Send a message to the queue.

        Args:
            data (Any): object of data to send (must be picklable)
        """
        raw_data = pickle.dumps(data, protocol=4)
        self.raw_pub_queue.send_message(raw_data)

    def recv(self, timeout: int = 60) -> MessageGeneratorContext:
        """Receive a stream of messages from the queue.

        This returns a context manager/ generator. It's iterator stops
        when no messages are received for `timeout` seconds. If an exception
        is raised, the message is rejected, but messages continue to
        be received (this is configured by `self._propagate_recv_error`).
        Multiple calls to `recv()` and/or recycling an instance are both okay,
        however if the queue has not been closed (e.g. premature termination
        of the iterator by a consumer-sider raised exception) the original
        parameters (`timeout`) are reused.

        Example:
            with queue.recv() as stream:
                for data in stream:
                    ...

        Keyword Arguments:
            timeout {int} -- seconds to wait idle before stopping (default: {60})

        Returns:
            MessageGeneratorContext -- context manager and generator object
        """
        if (not self.message_generator_context) or (not self._sub_queue) or self._sub_queue.was_closed:
            logging.debug("Creating new MessageGeneratorContext instance.")
            self.message_generator_context = MessageGeneratorContext(sub=self.raw_sub_queue,
                                                                     timeout=timeout,
                                                                     propagate_error=self._propagate_recv_error)
        return self.message_generator_context

    @contextlib.contextmanager
    def recv_one(self) -> Generator[Any, None, None]:
        """Receive one message from the queue.

        This is a context manager. If an exception is raised, the message is rejected.

        Decorators:
            contextlib.contextmanager

        Yields:
            Any -- object of data received, or None if queue is empty

        Raises:
            Exception -- [description]
        """
        msg = self.raw_sub_queue.get_message()
        if not msg:
            raise Exception('No message available')
        try:
            yield pickle.loads(msg.data)
        except Exception:
            self.raw_sub_queue.reject_message(msg.msg_id)
            raise
        else:
            self.raw_sub_queue.ack_message(msg.msg_id)
        finally:
            self.close()

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return f"Queue({self.backend.__class__.__name__}, address={self.address}, name={self.name}, prefetch={self.prefetch}, pub={bool(self._pub_queue)}, sub={bool(self._sub_queue)})"
