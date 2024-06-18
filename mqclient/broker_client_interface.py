"""Define an interface that broker_clients will adhere to."""

import json
import uuid
from enum import Enum, auto
from typing import Any, AsyncGenerator, Dict, Optional, Union

import zstd

from .config import MIN_PREFETCH

MessageID = Union[int, str, bytes]


class MQClientException(Exception):
    """Any exception for an error originating here."""


class ConnectingFailedException(MQClientException):
    """Raised when a `connect()` fails."""


class ClosingFailedException(MQClientException):
    """Raised when a `close()` fails."""


class AckException(MQClientException):
    """Raised when there's a problem with acking."""


class NackException(MQClientException):
    """Raised when there's a problem with nacking."""


class Message:
    """Message object.

    Holds msg_id and data.
    """

    class AckStatus(Enum):
        """Signify the ack state of a message."""

        NONE = auto()  # message has not been acked nor nacked
        ACKED = auto()  # message has been acked
        NACKED = auto()  # message has been nacked

    def __init__(self, msg_id: MessageID, payload: bytes):
        if not isinstance(msg_id, (int, str, bytes)):
            raise TypeError(
                f"Message.msg_id must be type int|str|bytes (not '{type(msg_id)}')."
            )
        if not isinstance(payload, bytes):
            raise TypeError(
                f"Message.data must be type 'bytes' (not '{type(payload)}')."
            )
        self.msg_id = msg_id
        self.payload = payload
        self._ack_status: Message.AckStatus = Message.AckStatus.NONE

        self._deserialized_payload: Dict[str, Any] = {}

        # set for special purposes since msg_id is not unique on redelivery
        self.uuid = int(uuid.uuid4())

        # set for special purposes which vary per broker api
        self._connection_id: Optional[int] = None

    def __repr__(self) -> str:
        """Return string of basic properties/attributes."""
        return f"Message(msg_id={self.msg_id!r}, payload={self.payload!r}, _ack_status={self._ack_status}, uuid={self.uuid}, _connection_id={self._connection_id})"

    def __eq__(self, other: object) -> bool:
        """Return True if self's and other's `data` are equal.

        On redelivery, `msg_id` may differ from its original, so
        `msg_id` is not a reliable source for testing equality. And
        neither is the `headers` field.
        """
        return bool(other) and isinstance(other, Message) and (self.data == other.data)

    @property
    def data(self) -> Any:
        """The object from the `data` field."""
        return self._deserialize()["data"]

    @property
    def headers(self) -> Any:
        """The dict from the `headers` field."""
        return self._deserialize()["headers"]

    def _deserialize(self) -> Dict[str, Any]:
        if not self._deserialized_payload:
            self._deserialized_payload = json.loads(zstd.decompress(self.payload))
        return self._deserialized_payload

    @staticmethod
    def serialize(data: Any, headers: Optional[Dict[str, Any]] = None) -> bytes:
        """Return serialized (bytes) representation of message payload.

        Optionally include `headers` dict for internal information.

        Data is compressed using `zstd.compress`, which was chosen by comparing
        the performance of bz2, lzma, zstd, gzip, and lz4 compression methods on
        various types of data.
        """
        if not headers:
            headers = {}

        return zstd.compress(
            json.dumps({"headers": headers, "data": data}).encode("utf-8"),
            3,  # level (3 is default)
            1,  # num of threads (auto is default)
        )


# -----------------------------
# classes to override/implement
# -----------------------------


class RawQueue:
    """Raw queue object, to hold queue state."""

    async def connect(self) -> None:
        """Set up connection."""

    async def close(self) -> None:
        """Close interface to queue."""


class Pub(RawQueue):
    """Publisher queue."""

    async def send_message(
        self,
        msg: bytes,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Send a message on a queue."""
        raise NotImplementedError()


class Sub(RawQueue):
    """Subscriber queue."""

    @property
    def prefetch(self) -> int:
        """Get prefetch."""
        return self._prefetch

    @prefetch.setter
    def prefetch(self, val: int) -> None:
        """Set prefetch."""
        if val < MIN_PREFETCH:
            raise ValueError(f"prefetch must be >= {MIN_PREFETCH}")
        self._prefetch = val

    @staticmethod
    def _to_message(*args: Any) -> Optional[Message]:
        """Convert broker_client-specific payload to standardized Message
        type."""
        raise NotImplementedError()

    async def get_message(
        self,
        timeout_millis: Optional[int],
        retries: int,
        retry_delay: float,
    ) -> Optional[Message]:
        """Get a single message from a queue."""
        raise NotImplementedError()

    async def ack_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Ack a message from the queue."""
        raise NotImplementedError()

    async def reject_message(
        self,
        msg: Message,
        retries: int,
        retry_delay: float,
    ) -> None:
        """Reject (nack) a message from the queue."""
        raise NotImplementedError()

    def message_generator(  # NOTE: no `async` b/c it's abstract; overriding methods will need `async`
        self,
        timeout: int,
        propagate_error: bool,
        retries: int,
        retry_delay: float,
    ) -> AsyncGenerator[Optional[Message], None]:
        """Yield Messages.

        Asynchronously generate messages with variable timeout.
        Yield `None` on `athrow()`.

        Keyword Arguments:
            timeout {int} -- timeout in seconds for inactivity (default: {60})
            propagate_error {bool} -- should errors from downstream code kill the generator? (default: {True})
        """
        raise NotImplementedError()


class BrokerClient:
    """BrokerClient Pub-Sub Factory."""

    NAME = "abstract-broker_client"

    @staticmethod
    async def create_pub_queue(
        address: str,
        name: str,
        auth_token: str,
    ) -> Pub:
        """Create a publishing queue."""
        raise NotImplementedError()

    @staticmethod
    async def create_sub_queue(
        address: str,
        name: str,
        prefetch: int,
        auth_token: str,
    ) -> Sub:
        """Create a subscription queue."""
        raise NotImplementedError()
