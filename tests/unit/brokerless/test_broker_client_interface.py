"""Unit test the broker_client interface."""

# fmt: off

# local imports
from mqclient import broker_client_interface


def test_RawQueue() -> None:
    """Test RawQueue."""
    assert hasattr(broker_client_interface, "RawQueue")


def test_Message() -> None:
    """Test Message."""
    m = broker_client_interface.Message('foo', b'abc')
    assert m.msg_id == 'foo'
    assert m.payload == b'abc'
    assert m._ack_status == broker_client_interface.Message.AckStatus.NONE
