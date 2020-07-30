"""Unit test the backend interface."""

# local imports
from MQClient import backend_interface


def test_RawQueue() -> None:
    """Test RawQueue."""
    assert hasattr(backend_interface, "RawQueue")


def test_Message() -> None:
    """Test Message."""
    m = backend_interface.Message('foo', b'abc')
    assert m.msg_id == 'foo'
    assert m.data == b'abc'
