# import pytest  # type: ignore

from MQClient import backend_interface

# @pytest.fixture

# @pytest.mark.asyncio
def test_RawQueue() -> None:
    assert hasattr(backend_interface, "RawQueue")

def test_Message() -> None:
    m = backend_interface.Message('foo', b'abc')
    assert m.msg_id == 'foo'
    assert m.data == b'abc'
