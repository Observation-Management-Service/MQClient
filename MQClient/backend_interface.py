import typing

class RawQueue:
    """
    Generic queue interface to a backend implementation.

    Args:
        name (str): name of queue
        address (str): address of queue server
    """
    def __init__(self, name: str, address: str) -> None:
        raise NotImplementedError()

    def send(self, msg: bytes) -> None:
        """
        Send a message on the queue.

        Args:
            msg (bytes): message data to send
        """
        raise NotImplementedError()

    def recv(self, callback: typing.Callable[[bytes], None]) -> None:
        """
        Receive messages from the queue, via a callback function.
        """
        raise NotImplementedError()
