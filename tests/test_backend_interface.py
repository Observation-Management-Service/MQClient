# import pytest  # type: ignore

from MQClient import backend_interface

# @pytest.fixture

# @pytest.mark.asyncio
def test_interface() -> None:
    q = backend_interface.RawQueue
    assert hasattr(q, "send")
    assert hasattr(q, "recv")
