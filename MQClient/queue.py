"""Queue class encapsulating a pub-sub messaging system."""

import contextlib
import pickle
import typing
import uuid


class Queue:
    """
    User-facing queue library.

    Args:
        backend (module): the backend to use
        address (str): address of queue (default: 'localhost')
        name (str): name of queue (default: random string)
        prefetch (int): size of prefetch buffer for receiving messages (default: 10)
    """

    def __init__(self, backend: typing.Any, address: str = 'localhost',
                 name: str = None, prefetch: int = 1) -> None:
        self._backend = backend
        self._address = address
        self._name = name if name else uuid.uuid4().hex
        self._prefetch = prefetch
        self._pub_queue = None
        self._sub_queue = None

    @property
    def backend(self) -> typing.Any:
        return self._backend

    @property
    def address(self) -> str:
        return self._address

    @property
    def name(self) -> str:
        return self._name

    @property
    def prefetch(self) -> int:
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
    def raw_pub_queue(self) -> typing.Any:
        if not self._pub_queue:
            self._pub_queue = self._backend.create_pub_queue(self._address, self._name)
        return self._pub_queue

    @raw_pub_queue.deleter
    def raw_pub_queue(self) -> None:
        self._close_pub_queue()

    def _close_pub_queue(self):
        if self._pub_queue:
            self._pub_queue.close()
            self._pub_queue = None

    @property
    def raw_sub_queue(self) -> typing.Any:
        if not self._sub_queue:
            self._sub_queue = self._backend.create_sub_queue(self._address, self._name, self._prefetch)
        return self._sub_queue

    @raw_sub_queue.deleter
    def raw_sub_queue(self) -> None:
        self._close_sub_queue()

    def _close_sub_queue(self) -> None:
        if self._sub_queue:
            self._sub_queue.close()
            self._sub_queue = None

    def close(self) -> None:
        """Close all connections."""
        self._close_sub_queue()
        self._close_pub_queue()

    def send(self, data: typing.Any) -> None:
        """
        Send a message to the queue.

        Args:
            data (Any): object of data to send (must be picklable)
        """
        raw_data = pickle.dumps(data, protocol=4)
        self._backend.send_message(self.raw_pub_queue, raw_data)

    def recv(self, timeout: int = 60) -> typing.Generator[typing.Any, None, None]:
        """
        Receive a stream of messages from the queue.

        This is a generator. It stops when no messages are received
        for `timeout` seconds. If an exception is raised, the message
        is rejected, but messages continue to be received.

        Args:
            timeout (int): seconds to wait idle before stopping (default: 60)
        Yields:
            Any: object of data received
        """
        for msg in self._backend.message_generator(self.raw_sub_queue, timeout=timeout, propagate_error=False):
            data = pickle.loads(msg.data)
            yield data

    @contextlib.contextmanager
    def recv_one(self) -> typing.Generator[typing.Any, None, None]:
        """
        Receive one message from the queue.

        This is a context manager. If an exception is raised, the message
        is rejected.

        Returns:
            Any: object of data received, or None if queue is empty
        """
        msg = self._backend.get_message(self.raw_sub_queue)
        if not msg:
            raise Exception('No message available')
        try:
            yield pickle.loads(msg.data)
        except Exception:
            self._backend.reject_message(self.raw_sub_queue, msg.msg_id)
            raise
        else:
            self._backend.ack_message(self.raw_sub_queue, msg.msg_id)
        finally:
            self.close()
