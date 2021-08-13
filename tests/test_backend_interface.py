"""Unit test the backend interface."""

# fmt: off

# local imports
from mqclient import backend_interface


def test_RawQueue() -> None:
    """Test RawQueue."""
    assert hasattr(backend_interface, "RawQueue")


def test_Message() -> None:
    """Test Message."""
    m = backend_interface.Message('foo', b'abc')
    assert m.msg_id == 'foo'
    assert m.payload == b'abc'
