"""Convenience wrapper around wipac-telemetry, so package can be used with/without it.

Based on https://github.com/WIPACrepo/rest-tools/blob/master/rest_tools/telemetry.py
"""

# pylint:skip-file

from enum import Enum, auto
from typing import Any, Callable, Dict, Optional, TypeVar, cast

#
# First, try to import then implement wipac-telemetry
#
try:
    import wipac_telemetry.tracing_tools as wtt  # type: ignore[import]  # ignore for CI/CD

    evented = wtt.evented
    get_current_span = wtt.get_current_span
    inject_links_carrier = wtt.inject_links_carrier
    inject_span_carrier = wtt.inject_span_carrier
    respanned = wtt.respanned
    spanned = wtt.spanned

    CarrierRelation = wtt.CarrierRelation
    Span = wtt.Span
    SpanBehavior = wtt.SpanBehavior
    SpanKind = wtt.SpanKind
    SpanNamer = wtt.SpanNamer

    def set_current_span_attribute(key: str, value: Any) -> None:
        wtt.get_current_span().set_attribute(key, value)

    def inject_span_carrier_if_recording(carrier: Optional[Dict[str, Any]]) -> None:
        if wtt.get_current_span().is_recording():
            wtt.inject_span_carrier(carrier)


#
# Otherwise, dummy-implement every call
#
except ImportError:

    # fmt: off
    # See: https://stackoverflow.com/a/69030553
    F = TypeVar("F", bound=Callable[..., Any])

    def dummy_wrapper(*args: Any, **kwargs: Any) -> Callable[[F], F]:
        def decorator(fn: F) -> F:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)
            return cast(F, wrapper)
        return decorator
    # fmt:on

    evented = dummy_wrapper
    spanned = dummy_wrapper
    respanned = dummy_wrapper

    def dummy_func(*args: Any, **kwargs: Any) -> None:
        pass

    class DummyClass:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    SpanNamer = DummyClass  # type: ignore[assignment, misc]
    Span = DummyClass  # type: ignore[assignment, misc]

    class SpanKind(Enum):  # type: ignore[no-redef]
        INTERNAL = 0
        SERVER = 1
        CLIENT = 2
        PRODUCER = 3
        CONSUMER = 4

    class SpanBehavior(Enum):  # type: ignore[no-redef]
        END_ON_EXIT = auto()
        DONT_END = auto()
        ONLY_END_ON_EXCEPTION = auto()

    class CarrierRelation(Enum):  # type: ignore[no-redef]
        SPAN_CHILD = auto()
        LINK = auto()

    set_current_span_attribute = dummy_func
    inject_span_carrier_if_recording = dummy_func

    def inject_span_carrier(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {}

    def inject_links_carrier(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {}

    def get_current_span() -> Span:  # type: ignore[misc,valid-type] # ignore 'valid-type' for CI/CD
        return Span()  # type: ignore[abstract]
